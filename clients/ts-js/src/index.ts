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

// ==========================================
// UbiQ SDK Implementation
// ==========================================

export class UbiQ {
  static sidecarProcess: any = null;
  static sidecarConnected = false;
  static controlPort = 7448;
  static activeSubscribers = new Map(); // id -> callback
  static clientSockets = new Set();

  static async initialize() {
    const manifest = UbiQ.discoverModule();
    if (!manifest) {
      console.log("[ubiq-sdk] No messaging runtime module discovered.");
      return;
    }

    if (manifest.binary?.command === "quicmqd") {
      await UbiQ.ensureSidecarRunning();
    }
  }

  static discoverModule() {
    // Check local package workspaces or node_modules
    const paths = [
      path.join(process.cwd(), "Planes/Events/QUICMQ/ubiq.module.yml"),
      path.join(process.cwd(), "node_modules/@allascode/quicmq/ubiq.module.yml")
    ];
    for (const p of paths) {
      if (fs.existsSync(p)) {
        // Simple YAML parse of the manifest
        const content = fs.readFileSync(p, "utf-8");
        const lines = content.split("\n");
        const manifestObj: any = {};
        for (const line of lines) {
          const parts = line.split(":");
          if (parts.length >= 2) {
            const k = parts[0].trim();
            const v = parts.slice(1).join(":").trim();
            if (k && v) {
              if (k === "command") {
                if (!manifestObj.binary) manifestObj.binary = {};
                manifestObj.binary.command = v;
              }
              manifestObj[k] = v;
            }
          }
        }
        return manifestObj;
      }
    }
    return null;
  }

  static async ensureSidecarRunning(): Promise<void> {
    return new Promise((resolve) => {
      // Try to connect to check if it's already running
      const testSocket = net.createConnection({ port: UbiQ.controlPort, host: "127.0.0.1" }, () => {
        testSocket.end();
        UbiQ.sidecarConnected = true;
        resolve();
      });

      testSocket.on("error", () => {
        // Not running, spawn it
        const possiblePaths = [
          path.join(process.cwd(), "Planes/Events/QUICMQ/bin/quicmqd.js"),
          path.join(process.cwd(), "node_modules/@allascode/quicmq/bin/quicmqd.js")
        ];
        let scriptPath = "";
        for (const p of possiblePaths) {
          if (fs.existsSync(p)) {
            scriptPath = p;
            break;
          }
        }

        if (scriptPath) {
          console.log(`[ubiq-sdk] Spawning quicmqd sidecar: ${scriptPath}`);
          UbiQ.sidecarProcess = spawn("node", [scriptPath], {
            stdio: "ignore",
            detached: true
          });
          UbiQ.sidecarProcess.unref();

          // Poll connection until alive
          let attempts = 0;
          const poll = setInterval(() => {
            attempts++;
            const conn = net.createConnection({ port: UbiQ.controlPort, host: "127.0.0.1" }, () => {
              conn.end();
              clearInterval(poll);
              UbiQ.sidecarConnected = true;
              resolve();
            });
            conn.on("error", () => {
              if (attempts > 30) {
                clearInterval(poll);
                resolve(); // resolve anyway to avoid hanging
              }
            });
          }, 100);
        } else {
          console.warn("[ubiq-sdk] Could not locate quicmqd script to spawn.");
          resolve();
        }
      });
    });
  }

  static sendControlCommand(cmdObj: any): Promise<any> {
    return new Promise((resolve, reject) => {
      const client = net.createConnection({ port: UbiQ.controlPort, host: "127.0.0.1" }, () => {
        client.write(JSON.stringify(cmdObj) + "\n");
      });
      
      let buffer = "";
      client.on("data", (data: Buffer | string) => {
        buffer += data.toString();
        if (buffer.includes("\n")) {
          client.end();
          try {
            resolve(JSON.parse(buffer.split("\n")[0]));
          } catch (err) {
            reject(err);
          }
        }
      });

      client.on("error", (err: Error) => {
        reject(err);
      });
    });
  }

  static async publish(event: string, payload: any, entity_id?: string) {
    await UbiQ.initialize();
    const res = await UbiQ.sendControlCommand({
      command: "publish",
      event,
      payload,
      entity_id
    });
    if (res.error) {
      throw new Error(res.error);
    }
    return res;
  }

  static async subscribe(event: string, ...args: any[]) {
    await UbiQ.initialize();
    
    let options = {};
    let callback: any = null;
    if (args.length === 2) {
      options = args[0];
      callback = args[1];
    } else {
      callback = args[0];
    }

    const subscriberId = `sub_${crypto.randomUUID()}`;
    UbiQ.activeSubscribers.set(subscriberId, callback);

    // Register with sidecar
    const res = await UbiQ.sendControlCommand({
      command: "register_subscriber",
      id: subscriberId,
      address: "127.0.0.1",
      event
    });

    if (res.error) {
      throw new Error(res.error);
    }

    // Connect persistent socket to receive deliveries
    const socket = net.createConnection({ port: UbiQ.controlPort, host: "127.0.0.1" }, () => {
      // Keep connection open
    });

    UbiQ.clientSockets.add(socket);

    let buffer = "";
    socket.on("data", async (data: Buffer | string) => {
      buffer += data.toString();
      if (buffer.includes("\n")) {
        const parts = buffer.split("\n");
        const msgStr = parts[0];
        buffer = parts.slice(1).join("\n");
        try {
          const msgObj = JSON.parse(msgStr);
          if (msgObj.type === "delivery" && msgObj.subscriber_id === subscriberId) {
            // Step 1: ACK the delivery
            const ackRes = await UbiQ.sendControlCommand({
              command: "ack",
              delivery_id: msgObj.delivery_id
            });

            if (ackRes.error) {
              console.error(`[ubiq-sdk] ACK failed: ${ackRes.error}`);
              return;
            }

            const decypherKey = ackRes.decypher_key;
            
            // Decrypt simulation (strip cipher(...) prefix)
            let payload = msgObj.payload_ciphertext;
            if (payload.startsWith("cipher(")) {
              payload = JSON.parse(payload.substring(7, payload.length - 1));
            }

            // Run application callback
            await callback({
              event_id: msgObj.event_id,
              delivery_id: msgObj.delivery_id,
              payload
            });

            // Step 2: Mark as consumed
            const consumedRes = await UbiQ.sendControlCommand({
              command: "consumed",
              delivery_id: msgObj.delivery_id,
              decypher_key: decypherKey
            });

            if (consumedRes.error) {
              console.error(`[ubiq-sdk] Consumed failed: ${consumedRes.error}`);
            }
          }
        } catch (err) {
          // ignore parsing error for non-json
        }
      }
    });

    socket.on("close", () => {
      UbiQ.clientSockets.delete(socket);
    });

    return {
      unsubscribe: async () => {
        socket.end();
        await UbiQ.sendControlCommand({
          command: "unregister_subscriber",
          id: subscriberId
        });
        UbiQ.activeSubscribers.delete(subscriberId);
      }
    };
  }

  static agent(name: string, agentConfig: { schema: string; graflow: string; counter: number }) {
    return {
      subscribe: async (event: string, callback: any) => {
        await UbiQ.initialize();

        // Compute Agent hashes
        const canonicalSchema = agentConfig.schema;
        const canonicalGraflow = agentConfig.graflow;

        const computeSha256 = (data: string) => crypto.createHash("sha256").update(data).digest("hex");
        const schemaHash = computeSha256(canonicalSchema);
        const graflowHash = computeSha256(canonicalGraflow);
        const agentHash = computeSha256(`${name}:${schemaHash}:${graflowHash}`);
        const serverId = "official";
        const agentInstanceId = computeSha256(`${serverId}:${agentHash}:${agentConfig.counter}`);

        UbiQ.activeSubscribers.set(agentInstanceId, callback);

        // Register agent subscriber
        const res = await UbiQ.sendControlCommand({
          command: "register_subscriber",
          id: agentInstanceId,
          address: "127.0.0.1",
          event,
          server_id: serverId,
          agent_hash: agentHash,
          counter: agentConfig.counter
        });

        if (res.error) {
          throw new Error(res.error);
        }

        const socket = net.createConnection({ port: UbiQ.controlPort, host: "127.0.0.1" });
        UbiQ.clientSockets.add(socket);

        let buffer = "";
        socket.on("data", async (data: Buffer | string) => {
          buffer += data.toString();
          if (buffer.includes("\n")) {
            const parts = buffer.split("\n");
            const msgStr = parts[0];
            buffer = parts.slice(1).join("\n");
            try {
              const msgObj = JSON.parse(msgStr);
              if (msgObj.type === "delivery" && msgObj.subscriber_id === agentInstanceId) {
                const ackRes = await UbiQ.sendControlCommand({
                  command: "ack",
                  delivery_id: msgObj.delivery_id
                });
                const decypherKey = ackRes.decypher_key;

                let payload = msgObj.payload_ciphertext;
                if (payload.startsWith("cipher(")) {
                  payload = JSON.parse(payload.substring(7, payload.length - 1));
                }

                await callback({
                  event_id: msgObj.event_id,
                  delivery_id: msgObj.delivery_id,
                  payload
                });

                await UbiQ.sendControlCommand({
                  command: "consumed",
                  delivery_id: msgObj.delivery_id,
                  decypher_key: decypherKey
                });
              }
            } catch {
              // ignore
            }
          }
        });

        socket.on("close", () => {
          UbiQ.clientSockets.delete(socket);
        });

        return {
          unsubscribe: async () => {
            socket.end();
            await UbiQ.sendControlCommand({
              command: "unregister_subscriber",
              id: agentInstanceId,
              server_id: serverId,
              agent_hash: agentHash,
              counter: agentConfig.counter
            });
            UbiQ.activeSubscribers.delete(agentInstanceId);
          }
        };
      }
    };
  }

  static async shutdown() {
    for (const socket of UbiQ.clientSockets as any) {
      socket.end();
    }
    UbiQ.clientSockets.clear();
    try {
      await UbiQ.sendControlCommand({ command: "shutdown" });
    } catch {
      // ignore if already shut down
    }
  }
}
