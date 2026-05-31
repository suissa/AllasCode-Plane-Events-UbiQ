import type { ConnectionOptions, Server } from "nats/lib/nats-base-client/core.js";
import type { Transport } from "nats/lib/nats-base-client/transport.js";

type WebTransportConstructor = new (url: string, options?: unknown) => WebTransportLike;

type WebTransportLike = {
  readonly ready: Promise<void>;
  readonly closed: Promise<unknown>;
  createBidirectionalStream(): Promise<{
    readable: ReadableStream<Uint8Array>;
    writable: WritableStream<Uint8Array>;
  }>;
  readonly incomingBidirectionalStreams?: ReadableStream<{
    readable: ReadableStream<Uint8Array>;
    writable: WritableStream<Uint8Array>;
  }>;
  close(closeInfo?: { closeCode?: number; reason?: string }): void;
};

export interface QuicAdapterOptions {
  path?: string;
  webTransport?: WebTransportConstructor;
  webTransportOptions?: unknown;
  url?: (server: Server) => string;
}

export interface QuicConnectionOptions extends ConnectionOptions {
  mode?: "TCP" | "QUIC";
  quic?: QuicAdapterOptions;
}

const DEFAULT_QUIC_PATH = "/nats";
const VERSION = "2.29.3";
const LANG = "nats.js-quic";

export class QuicTransport implements Transport {
  readonly version = VERSION;
  readonly lang = LANG;
  private webTransport?: WebTransportLike;
  private writer?: WritableStreamDefaultWriter<Uint8Array>;
  private reader?: ReadableStreamDefaultReader<Uint8Array>;
  private pending: Uint8Array[] = [];
  private waiting?: () => void;
  private closedPromise: Promise<void | Error>;
  private resolveClosed!: (value?: void | Error) => void;
  private done = false;
  closeError?: Error;

  constructor(private readonly adapterOptions: QuicAdapterOptions = {}) {
    this.closedPromise = new Promise((resolve) => {
      this.resolveClosed = resolve;
    });
  }

  get isClosed(): boolean {
    return this.done;
  }

  async connect(server: Server, _opts: ConnectionOptions): Promise<void> {
    const WebTransportImpl = (this.adapterOptions.webTransport ?? globalThis.WebTransport) as WebTransportConstructor | undefined;
    if (!WebTransportImpl) {
      throw new Error("QUIC mode requires a WebTransport implementation. Provide quic.webTransport or run on a runtime with global WebTransport support.");
    }

    const transport = new WebTransportImpl(this.buildUrl(server), this.adapterOptions.webTransportOptions);
    this.webTransport = transport;
    await transport.ready;

    const stream = await this.openNatsStream(transport);
    this.reader = stream.readable.getReader();
    this.writer = stream.writable.getWriter();
    this.done = false;

    void this.readLoop();
    void transport.closed.then(
      () => this.finish(),
      (err) => this.finish(err instanceof Error ? err : new Error(String(err))),
    );
  }

  private async openNatsStream(transport: WebTransportLike): Promise<{
    readable: ReadableStream<Uint8Array>;
    writable: WritableStream<Uint8Array>;
  }> {
    const incoming = transport.incomingBidirectionalStreams?.getReader();
    if (incoming) {
      const { value, done } = await incoming.read();
      incoming.releaseLock();
      if (!done && value) return value;
    }
    return transport.createBidirectionalStream();
  }

  [Symbol.asyncIterator](): AsyncIterableIterator<Uint8Array> {
    return this.iterate();
  }

  isEncrypted(): boolean {
    return true;
  }

  send(frame: Uint8Array): void {
    if (this.done || !this.writer) return;
    void this.writer.write(frame).catch((err) => this.finish(err instanceof Error ? err : new Error(String(err))));
  }

  async close(err?: Error): Promise<void> {
    this.closeError = err;
    await this.finish(err);
  }

  disconnect(): void {
    void this.close();
  }

  closed(): Promise<void | Error> {
    return this.closedPromise;
  }

  discard(): void {
    this.pending.length = 0;
  }

  private buildUrl(server: Server): string {
    if (this.adapterOptions.url) return this.adapterOptions.url(server);
    const path = normalizePath(this.adapterOptions.path ?? DEFAULT_QUIC_PATH);
    return `https://${server.listen}${path}`;
  }

  private async readLoop(): Promise<void> {
    if (!this.reader) return;
    try {
      for (;;) {
        const { value, done } = await this.reader.read();
        if (done) break;
        if (value) {
          this.pending.push(value);
          this.waiting?.();
        }
      }
      await this.finish();
    } catch (err) {
      await this.finish(err instanceof Error ? err : new Error(String(err)));
    }
  }

  private async *iterate(): AsyncIterableIterator<Uint8Array> {
    while (!this.done || this.pending.length > 0) {
      const frame = this.pending.shift();
      if (frame) {
        yield frame;
        continue;
      }
      await new Promise<void>((resolve) => {
        this.waiting = resolve;
      });
      this.waiting = undefined;
    }
  }

  private async finish(err?: Error): Promise<void> {
    if (this.done) return;
    this.done = true;
    this.closeError = err;
    try {
      await this.writer?.close();
    } catch {
      // ignore close races
    }
    try {
      this.reader?.releaseLock();
    } catch {
      // ignore release races
    }
    try {
      this.webTransport?.close(err ? { closeCode: 1, reason: err.message } : undefined);
    } catch {
      // ignore close races
    }
    this.waiting?.();
    this.resolveClosed(err);
  }
}

export function createQuicTransportFactory(options?: QuicAdapterOptions): () => Transport {
  return () => new QuicTransport(options);
}

export function parseQuicServerUrl(url: string): string {
  return url.replace(/^(nats\+quic|quic):\/\//i, "");
}

function normalizePath(path: string): string {
  return path.startsWith("/") ? path : `/${path}`;
}
