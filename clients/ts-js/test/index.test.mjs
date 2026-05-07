import assert from "node:assert/strict";
import test from "node:test";
import { headers } from "nats";
import { LINEAR_DLQ_ORIGINAL_SUBJECT_HEADER, LINEAR_DLQ_REASON_HEADER, LINEAR_EVENT_HEADER, LINEAR_EVENT_TYPE, LINEAR_OUTBOX_ID_HEADER, LINEAR_TTL_HEADER, LinearMessage, Outbox } from "../dist/index.js";

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
