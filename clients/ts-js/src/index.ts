import { connect as connectNats, headers, type Msg, type NatsConnection, type Subscription } from "nats";
import { NatsConnectionImpl } from "nats/lib/nats-base-client/nats.js";
import { setTransportFactory } from "nats/lib/nats-base-client/transport.js";
import { createQuicTransportFactory, parseQuicServerUrl, type QuicConnectionOptions } from "./adapters/quic.js";


export type LinearConnectionMode = "TCP" | "QUIC";
export type { QuicAdapterOptions, QuicConnectionOptions } from "./adapters/quic.js";
export { QuicTransport, createQuicTransportFactory, parseQuicServerUrl } from "./adapters/quic.js";

export async function connectLinear(options: QuicConnectionOptions = {}): Promise<NatsConnection> {
  const { mode = "TCP", quic, ...natsOptions } = options;
  if (mode === "TCP") return connectNats(natsOptions);

  setTransportFactory({
    factory: createQuicTransportFactory(quic),
    defaultPort: 4222,
    urlParseFn: parseQuicServerUrl,
  });

  return NatsConnectionImpl.connect(natsOptions);
}

export const LINEAR_EVENT_HEADER = "Nats-Event-Type";
export const LINEAR_EVENT_TYPE = "Linear";
export const LINEAR_TTL_HEADER = "Nats-Linear-TTL";
export const LINEAR_OUTBOX_ID_HEADER = "Nats-Linear-Outbox-Id";
export const LINEAR_DLQ_REASON_HEADER = "Nats-Linear-DLQ-Reason";
export const LINEAR_DLQ_ORIGINAL_SUBJECT_HEADER = "Nats-Linear-Original-Subject";
export const LINEAR_PQC_ALGORITHM_HEADER = "Nats-Linear-PQC-Alg";
export const LINEAR_PQC_PUBLIC_KEY_HEADER = "Nats-Linear-PQC-Public-Key";
export const DPOP_HEADER = "DPoP";
export const LINEAR_PQC_ALGORITHM = "ML-KEM-768";

export class LinearMessage {
  readonly subject: string;
  readonly reply?: string;
  readonly headers = new Map<string, string[]>();
  private payload?: Uint8Array;
  private timer?: ReturnType<typeof setTimeout>;
  private readonly linear: boolean;

  constructor(msg: Msg) {
    this.subject = msg.subject;
    this.reply = msg.reply;
    this.payload = new Uint8Array(msg.data);
    const eventType = msg.headers?.get(LINEAR_EVENT_HEADER);
    this.linear = eventType === LINEAR_EVENT_TYPE;
    const ttlValue = msg.headers?.get(LINEAR_TTL_HEADER);
    if (eventType) this.headers.set(LINEAR_EVENT_HEADER, [eventType]);
    if (ttlValue) this.headers.set(LINEAR_TTL_HEADER, [ttlValue]);
    const ttl = Number(ttlValue);
    if (this.linear && Number.isFinite(ttl) && ttl > 0) {
      this.timer = setTimeout(() => this.destroy(), ttl);
    }
  }

  access(): Uint8Array | undefined {
    if (!this.payload) return undefined;
    const value = new Uint8Array(this.payload);
    if (this.linear) this.destroy();
    return value;
  }

  destroy(): void {
    if (this.timer) clearTimeout(this.timer);
    this.timer = undefined;
    this.payload?.fill(0);
    this.payload = undefined;
  }
}

export interface SecurityOptions {
  dpopToken?: string;
  pqcPublicKey?: string;
}

export async function publishLinear(nc: NatsConnection, subject: string, payload: Uint8Array, ttlMs?: number): Promise<void> {
  publishLinearWithSecurity(nc, subject, payload, ttlMs);
}

export function publishLinearWithSecurity(nc: NatsConnection, subject: string, payload: Uint8Array, ttlMs?: number, security?: SecurityOptions): void {
  const h = headers();
  h.set(LINEAR_EVENT_HEADER, LINEAR_EVENT_TYPE);
  if (ttlMs && ttlMs > 0) h.set(LINEAR_TTL_HEADER, String(ttlMs));
  applySecurityHeaders(h, security);
  nc.publish(subject, payload, { headers: h });
}

export async function subscribeLinear(nc: NatsConnection, subject: string, cb: (msg: LinearMessage) => void): Promise<Subscription> {
  const sub = nc.subscribe(subject);
  (async () => {
    for await (const msg of sub) {
      cb(new LinearMessage(msg));
    }
  })();
  return sub;
}


export interface OutboxOptions {
  maxAttempts?: number;
  dlqSubject?: string;
  security?: SecurityOptions;
}

export interface OutboxEntry {
  id: string;
  subject: string;
  payload: Uint8Array;
  ttlMs?: number;
  attempts: number;
}

export class Outbox {
  private readonly entries: OutboxEntry[] = [];
  private readonly maxAttempts: number;
  private readonly dlqSubject?: string;
  private readonly security?: SecurityOptions;
  private nextId = 0;

  constructor(private readonly nc: NatsConnection, options: OutboxOptions = {}) {
    this.maxAttempts = Math.max(1, options.maxAttempts ?? 3);
    this.dlqSubject = options.dlqSubject;
    this.security = options.security;
  }

  enqueueLinear(subject: string, payload: Uint8Array, ttlMs?: number): string {
    const id = String(++this.nextId);
    this.entries.push({ id, subject, payload: new Uint8Array(payload), ttlMs, attempts: 0 });
    return id;
  }

  get length(): number {
    return this.entries.length;
  }

  flush(): void {
    for (let index = 0; index < this.entries.length;) {
      const entry = this.entries[index];
      try {
        this.nc.publish(entry.subject, entry.payload, { headers: linearHeaders(entry, this.security) });
        this.entries.splice(index, 1);
      } catch (err) {
        entry.attempts += 1;
        if (entry.attempts >= this.maxAttempts && this.dlqSubject) {
          this.nc.publish(this.dlqSubject, entry.payload, { headers: dlqHeaders(entry, err) });
          this.entries.splice(index, 1);
          continue;
        }
        throw err;
      }
    }
  }
}

function linearHeaders(entry: OutboxEntry, security?: SecurityOptions) {
  const h = headers();
  h.set(LINEAR_EVENT_HEADER, LINEAR_EVENT_TYPE);
  h.set(LINEAR_OUTBOX_ID_HEADER, entry.id);
  if (entry.ttlMs && entry.ttlMs > 0) h.set(LINEAR_TTL_HEADER, String(entry.ttlMs));
  applySecurityHeaders(h, security);
  return h;
}

function applySecurityHeaders(h: ReturnType<typeof headers>, security?: SecurityOptions): void {
  if (!security) return;
  if (security.pqcPublicKey) {
    h.set(LINEAR_PQC_ALGORITHM_HEADER, LINEAR_PQC_ALGORITHM);
    h.set(LINEAR_PQC_PUBLIC_KEY_HEADER, security.pqcPublicKey);
  }
  if (security.dpopToken) h.set(DPOP_HEADER, security.dpopToken);
}

function dlqHeaders(entry: OutboxEntry, err: unknown) {
  const h = headers();
  h.set(LINEAR_OUTBOX_ID_HEADER, entry.id);
  h.set(LINEAR_DLQ_ORIGINAL_SUBJECT_HEADER, entry.subject);
  h.set(LINEAR_DLQ_REASON_HEADER, err instanceof Error ? err.message : String(err));
  return h;
}
