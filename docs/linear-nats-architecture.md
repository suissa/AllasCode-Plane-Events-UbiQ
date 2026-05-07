# Linear NATS architecture, clients, outbox, and DLQ

This document describes the custom Linear Event layer added on top of the NATS server in this repository. It covers the architecture, technologies, runtime flow, client helpers, the outbox pattern, the dead-letter queue (DLQ), and operational guidance.

## Goals

- Preserve regular NATS behavior unless a publisher explicitly opts in to linear delivery.
- Allow a publisher to mark a message as `Linear` so the server delivers it to one matching subscriber instead of faning it out to every plain subscriber.
- Allow helper clients to keep a received linear payload only until the first successful access.
- Allow helper clients to destroy unread linear payloads after a TTL.
- Provide an outbox abstraction so producers can enqueue linear publishes locally and retry them.
- Provide a DLQ path so an outbox entry that cannot be published after the configured attempts is sent to an operator-visible subject.

## Technologies used

| Area | Technology | Role |
| --- | --- | --- |
| Server | Go | NATS server implementation and protocol routing logic. |
| Protocol | Core NATS + headers | Linear delivery is signaled with NATS headers; unmarked NATS messages remain unchanged. |
| Go client | `github.com/nats-io/nats.go` | Publishes and subscribes to linear events, plus outbox/DLQ helpers. |
| Rust client | `async-nats`, `tokio`, `bytes`, `futures` | Async linear publish/subscribe helpers, TTL cleanup, outbox modeling, and tests. |
| TS/JS client | `nats`, TypeScript, Node test runner | Linear publish/subscribe helpers, TTL cleanup, outbox/DLQ helpers, and tests. |
| Gleam client | Gleam, gleeunit, npm script wrapper | Client generated from the behavior JSON to validate language-neutral generation and TDD scenarios. |
| Security | mTLS, ML-KEM-768/Kyber, DPoP | mTLS authenticates the transport, ML-KEM-768 advertises ephemeral post-quantum key material for linear events, and DPoP binds publishes to a proof token. |

## Header contract

The feature is intentionally header-driven, so older clients and normal NATS messages continue to work.

| Header | Value | Set by | Meaning |
| --- | --- | --- | --- |
| `Nats-Event-Type` | `Linear` | Publisher/client helper | Enables server-side linear delivery. |
| `Nats-Linear-TTL` | Milliseconds, or Go duration in Go consumers | Publisher/client helper | Client-side time-to-live for an unread retained payload. |
| `Nats-Linear-Outbox-Id` | Client-generated id | Outbox | Correlates original publish attempts and DLQ records. |
| `Nats-Linear-DLQ-Reason` | Error text | Outbox | Explains why the message was moved to DLQ. |
| `Nats-Linear-Original-Subject` | Subject name | Outbox | Records the original target subject for the DLQ payload. |
| `Nats-Linear-PQC-Alg` | `ML-KEM-768` | Security helper | Identifies the Kyber/ML-KEM parameter set used for ephemeral linear key material. |
| `Nats-Linear-PQC-Public-Key` | Base64url public key | Security helper | Carries the ephemeral post-quantum public key for the linear event. |
| `DPoP` | Compact proof JWT/token | Security helper | Provides a proof-of-possession binding for the publish. |

## Security model: mTLS, PQC, and DPoP

The transport security layer remains standard NATS TLS/mTLS. The Go helper exposes `Connect(url, security, opts...)`, which applies a caller-provided `tls.Config`; applications should configure client certificates, root CAs, server name validation, and minimum TLS versions there.

For post-quantum protection metadata, the Go helper uses Go's `crypto/mlkem` implementation of ML-KEM-768, the NIST-standardized successor to Kyber. When `EnablePQC` is set, every linear publish receives a fresh ephemeral ML-KEM-768 public key in `Nats-Linear-PQC-Public-Key` and an algorithm marker in `Nats-Linear-PQC-Alg`. The private decapsulation key is intentionally ephemeral and not retained by the outbox after header generation. Rust and TS/JS expose the same headers for runtimes that generate or obtain PQC key material externally.

For DPoP, the Go helper can generate ES256 proof JWTs using a P-256 key. The proof includes the method `NATS`, the subject as the URI binding, an issued-at timestamp, and a random JTI. Rust and TS/JS helpers accept externally supplied DPoP tokens and attach them to the same `DPoP` header.

These security headers are additive: existing NATS clients can ignore them, while security-aware consumers or gateways can validate them before processing the payload.

## Server-side architecture

The server remains a Core NATS server. The Linear Event behavior is an additional routing decision in the client message processing path:

1. A publisher sends an `HPUB`/header message with `Nats-Event-Type: Linear`.
2. The server parses the message as usual and matches subscriptions with the account sublist.
3. Before normal fanout, the server checks the parsed header block for `Nats-Event-Type`.
4. If the value is not `Linear`, all existing fanout, queue, route, leaf, and gateway behavior stays unchanged.
5. If the value is `Linear`, the server sets an internal processing flag for the match result.
6. Delivery stops immediately after one successful subscriber delivery.
7. Gateway fanout is skipped for already identified linear events to avoid duplicating the single-consumer contract across gateway links.

This design keeps the protocol compatible because linear delivery is opt-in and uses existing NATS header frames.

## Client-side architecture

Each helper client implements the same conceptual API:

- `publish` / `Publish` / `publishLinear` sets the linear headers and optional TTL.
- `subscribe` / `Subscribe` / `subscribeLinear` wraps incoming NATS messages as a linear-aware message object.
- `access` / `Access` returns a copy or owned view of the payload.
- For linear messages, first access destroys the helper's retained payload reference.
- For non-linear messages, access remains reusable so current behavior is preserved.
- If TTL expires before first access, the retained payload is destroyed and future access returns no payload.

The TTL is intentionally enforced by the client helper, not by the server. This means TTL is about local payload retention after delivery, not broker-side message expiration.

## Managed queue lifecycle

A linear queue should remain open after it is created. The Go helper's `StartLinearQueue` creates a queue subscription and a destroy-event subscription. The queue is drained and closed only when the configured destroy subject receives a NATS event, or when the caller's context is canceled. If no destroy event arrives and the network connection drops, reconnect attempts happen every `ReconnectEvery` interval; the default is one second. `ReconnectFor` configures the total reconnect window and is translated into the NATS client's max reconnect count.

This lifecycle is useful for long-lived linear workers: creating the queue does not imply ownership of its destruction; destruction is an explicit NATS event.

## Outbox pattern

The outbox is a producer-side reliability helper. It is not a replacement for JetStream persistence; it is a lightweight client-side buffer intended for applications that want a single place to collect pending linear publishes before trying to send them.

Outbox flow:

1. Application calls `enqueueLinear(subject, payload, ttl)` or the language equivalent.
2. The outbox copies the payload, assigns a monotonically increasing outbox id, and keeps the entry in memory.
3. Application calls `flush()`.
4. The outbox publishes the entry as a linear event with `Nats-Linear-Outbox-Id`.
5. If publish succeeds, the entry is removed from the outbox.
6. If publish fails, the attempt counter is incremented.
7. If attempts remain, the entry stays in the outbox and can be flushed again.
8. If attempts are exhausted and a DLQ subject is configured, the outbox publishes the payload to the DLQ subject with diagnostic headers.

The current helper implementations use in-memory outboxes. Production applications that need crash-safe producer guarantees should persist the outbox entries in their own storage before calling `flush()` or use JetStream where appropriate.

## DLQ pattern

The DLQ is an operator-visible NATS subject that receives messages the outbox could not publish to their original subject after the configured number of attempts.

A DLQ record contains:

- the original payload;
- `Nats-Linear-Outbox-Id`, to correlate with application logs;
- `Nats-Linear-Original-Subject`, to identify the original target;
- `Nats-Linear-DLQ-Reason`, to identify the publish failure.

Recommended DLQ subject naming:

- `$LINEAR.DLQ.<service>` for service-specific DLQs;
- `$LINEAR.DLQ.global` for a shared operational DLQ;
- include environment or tenant tokens if accounts are shared.

Recommended DLQ consumer behavior:

1. Subscribe to the DLQ subject.
2. Log or persist the diagnostic headers and payload.
3. Decide whether to replay, inspect manually, or discard.
4. If replaying, publish a new linear event with a new outbox id rather than reusing stale attempt metadata.

## End-to-end flow

```text
Producer application
  └─ enqueueLinear()             (copy payload into outbox)
      └─ flush()
          ├─ publish linear event to target subject
          │   └─ NATS server detects Nats-Event-Type: Linear
          │       └─ deliver to exactly one matching subscriber
          │           └─ subscriber helper retains payload until access or TTL
          └─ on repeated publish failure
              └─ publish original payload to DLQ subject with diagnostic headers
```

## Failure behavior

| Failure | Behavior |
| --- | --- |
| No `Nats-Event-Type: Linear` header | Message uses regular NATS fanout behavior. |
| Linear message has no matching subscriber | Core NATS no-interest behavior applies. |
| Client receives linear message but never accesses it | TTL destroys the helper-retained payload if TTL is set. |
| Client accesses linear payload twice | First access returns payload; second access returns no payload. |
| Outbox publish fails below max attempts | Entry remains queued and `flush()` can be retried. |
| Outbox publish fails at max attempts with DLQ configured | Entry is published to the DLQ subject and removed after DLQ publish succeeds. |
| Outbox publish fails with no DLQ configured | Entry remains queued for later retries. |

## Operational guidance

- Use clear subject conventions for linear events, for example `linear.<domain>.<event>`.
- Configure DLQ subjects per service or account to avoid mixing unrelated failures.
- Monitor DLQ subjects; a growing DLQ usually indicates connectivity, permission, or subject-routing problems.
- Treat in-memory outboxes as process-local reliability. Persist entries externally if messages must survive process restarts.
- Keep TTL values longer than the expected handler scheduling delay; too-short TTLs can destroy payloads before handlers get CPU time.
- Configure `ReconnectFor` on managed queues so workers reconnect every second only for the operationally acceptable recovery window.
- Treat DPoP and PQC headers as verifiable security metadata at application boundaries until all consumers enforce them.
- Use JetStream for broker-side retention, replay, and durable consumer workflows. Linear client TTL is local memory cleanup, not stream retention.

## Compatibility notes

- Existing NATS clients can still publish and subscribe normally.
- Existing subscribers that receive linear messages without using these helpers will simply see a normal NATS message with headers.
- The server-side single-subscriber behavior only activates when the publisher sets `Nats-Event-Type: Linear`.
- Non-linear messages are explicitly tested to preserve current fanout behavior.

## Implementation map

| Component | Path |
| --- | --- |
| Server linear header constants and routing | `server/client.go` |
| Server linear behavior tests | `server/linear_event_test.go` |
| Go client, outbox, DLQ, mTLS/PQC/DPoP, managed queue | `clients/go/linear.go` |
| Go client tests | `clients/go/linear_test.go` |
| Rust client, outbox, DLQ | `clients/rust/src/lib.rs` |
| TypeScript/JavaScript client, outbox, DLQ | `clients/ts-js/src/index.ts` |
| TypeScript/JavaScript tests | `clients/ts-js/test/index.test.mjs` |
| AI client behavior definition | `docs/linear-client-definition.json` |
| Gleam generated client and tests | `clients/gleam` |
