import assert from "node:assert/strict";
import test from "node:test";
import { headers } from "nats";
import { LINEAR_EVENT_HEADER, LINEAR_EVENT_TYPE, LINEAR_TTL_HEADER, LinearMessage } from "../dist/index.js";

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
