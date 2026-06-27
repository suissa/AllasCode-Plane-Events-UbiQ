import assert from "node:assert/strict";
import test from "node:test";
import { headers } from "nats";
import { DPOP_HEADER, LINEAR_DLQ_ORIGINAL_SUBJECT_HEADER, LINEAR_DLQ_REASON_HEADER, LINEAR_EVENT_HEADER, LINEAR_EVENT_TYPE, LINEAR_OUTBOX_ID_HEADER, LINEAR_PQC_ALGORITHM, LINEAR_PQC_ALGORITHM_HEADER, LINEAR_PQC_PUBLIC_KEY_HEADER, LINEAR_TTL_HEADER, LinearMessage, Outbox, QuicTransport, parseQuicServerUrl } from "../dist/index.js";

function msg(data, linear = false, ttlMs) {
  const h = headers();
  if (linear) h.set(LINEAR_EVENT_HEADER, LINEAR_EVENT_TYPE);
  if (ttlMs !== undefined) h.set(LINEAR_TTL_HEADER, String(ttlMs));
  return {
    subject: "linear.test",
    data: new TextEncoder().encode(data),
    headers: h,
  };
}

function text(data) {
  return data ? new TextDecoder().decode(data) : undefined;
}

test("linear access destroys payload without clearing returned value", () => {
  const linear = new LinearMessage(msg("secret", true));

  const first = linear.access();
  assert.equal(text(first), "secret");
  assert.equal(linear.access(), undefined);
});

test("linear TTL destroys unread payload", async () => {
  const linear = new LinearMessage(msg("expires", true, 10));

  await new Promise((resolve) => setTimeout(resolve, 50));
  assert.equal(linear.access(), undefined);
});

test("non-linear access is reusable and ignores TTL", async () => {
  const regular = new LinearMessage(msg("reusable", false, 10));

  await new Promise((resolve) => setTimeout(resolve, 50));
  assert.equal(text(regular.access()), "reusable");
  assert.equal(text(regular.access()), "reusable");
});


test("outbox publishes queued linear messages and removes them", () => {
  const published = [];
  const nc = { publish: (subject, data, options) => published.push({ subject, data, options }) };
  const outbox = new Outbox(nc);

  const id = outbox.enqueueLinear("linear.out", new TextEncoder().encode("payload"), 25);
  outbox.flush();

  assert.equal(id, "1");
  assert.equal(outbox.length, 0);
  assert.equal(published.length, 1);
  assert.equal(published[0].subject, "linear.out");
  assert.equal(text(published[0].data), "payload");
  assert.equal(published[0].options.headers.get(LINEAR_EVENT_HEADER), LINEAR_EVENT_TYPE);
  assert.equal(published[0].options.headers.get(LINEAR_OUTBOX_ID_HEADER), "1");
  assert.equal(published[0].options.headers.get(LINEAR_TTL_HEADER), "25");
});

test("outbox moves failed messages to DLQ after max attempts", () => {
  const published = [];
  const nc = {
    publish: (subject, data, options) => {
      published.push({ subject, data, options });
      if (published.length === 1) throw new Error("publish failed");
    },
  };
  const outbox = new Outbox(nc, { maxAttempts: 1, dlqSubject: "linear.dlq" });

  outbox.enqueueLinear("linear.out", new TextEncoder().encode("payload"));
  outbox.flush();

  assert.equal(outbox.length, 0);
  assert.equal(published.length, 2);
  assert.equal(published[1].subject, "linear.dlq");
  assert.equal(published[1].options.headers.get(LINEAR_DLQ_ORIGINAL_SUBJECT_HEADER), "linear.out");
  assert.equal(published[1].options.headers.get(LINEAR_DLQ_REASON_HEADER), "publish failed");
});


test("outbox applies supplied PQC and DPoP security headers", () => {
  const published = [];
  const nc = { publish: (subject, data, options) => published.push({ subject, data, options }) };
  const outbox = new Outbox(nc, { security: { dpopToken: "proof.jwt", pqcPublicKey: "kyber-public-key" } });

  outbox.enqueueLinear("linear.secure", new TextEncoder().encode("payload"));
  outbox.flush();

  assert.equal(published[0].options.headers.get(LINEAR_PQC_ALGORITHM_HEADER), LINEAR_PQC_ALGORITHM);
  assert.equal(published[0].options.headers.get(LINEAR_PQC_PUBLIC_KEY_HEADER), "kyber-public-key");
  assert.equal(published[0].options.headers.get(DPOP_HEADER), "proof.jwt");
});


test("quic adapter strips QUIC protocols for NATS server parsing", () => {
  assert.equal(parseQuicServerUrl("quic://127.0.0.1:4222"), "127.0.0.1:4222");
  assert.equal(parseQuicServerUrl("nats+quic://example.com:4222"), "example.com:4222");
});

test("quic adapter sends and receives raw NATS protocol frames", async () => {
  const received = [];
  let controller;
  let closeTransport;
  const readable = new ReadableStream({
    start(c) {
      controller = c;
    },
  });
  const writable = new WritableStream({
    write(chunk) {
      received.push(new Uint8Array(chunk));
    },
  });

  class FakeWebTransport {
    constructor(url) {
      this.url = url;
      this.ready = Promise.resolve();
      this.closed = new Promise((resolve) => {
        closeTransport = resolve;
      });
      this.incomingBidirectionalStreams = new ReadableStream({
        start(controller) {
          controller.enqueue({ readable, writable });
          controller.close();
        },
      });
    }

    async createBidirectionalStream() {
      throw new Error("expected server-opened stream");
    }

    close() {
      closeTransport();
    }
  }

  const transport = new QuicTransport({ webTransport: FakeWebTransport, path: "nats" });
  await transport.connect({ listen: "localhost:4222" }, {});

  transport.send(new TextEncoder().encode("PING\r\n"));
  assert.equal(new TextDecoder().decode(received[0]), "PING\r\n");

  const next = transport[Symbol.asyncIterator]().next();
  controller.enqueue(new TextEncoder().encode("PONG\r\n"));
  const delivered = await next;
  assert.equal(new TextDecoder().decode(delivered.value), "PONG\r\n");

  await transport.close();
});
