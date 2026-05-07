import { headers, type Msg, type NatsConnection, type Subscription } from "nats";

export const LINEAR_EVENT_HEADER = "Nats-Event-Type";
export const LINEAR_EVENT_TYPE = "Linear";
export const LINEAR_TTL_HEADER = "Nats-Linear-TTL";

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

export async function publishLinear(nc: NatsConnection, subject: string, payload: Uint8Array, ttlMs?: number): Promise<void> {
  const h = headers();
  h.set(LINEAR_EVENT_HEADER, LINEAR_EVENT_TYPE);
  if (ttlMs && ttlMs > 0) h.set(LINEAR_TTL_HEADER, String(ttlMs));
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
