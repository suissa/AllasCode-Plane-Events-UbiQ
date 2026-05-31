# Linear NATS clients

These helper clients use NATS headers to mark an event as `Linear` and provide a lightweight producer outbox plus DLQ integration.

## Headers

- `Nats-Event-Type: Linear` enables linear delivery on this server build.
- `Nats-Linear-TTL: <milliseconds>` is forwarded to clients so they can destroy an unread payload after a time-to-live.
- `Nats-Linear-Outbox-Id` correlates outbox publish attempts and DLQ records.
- `Nats-Linear-DLQ-Reason` explains why a message was moved to DLQ.
- `Nats-Linear-Original-Subject` records the original target subject on DLQ messages.
- `Nats-Linear-PQC-Alg: ML-KEM-768` identifies the post-quantum KEM used for an ephemeral linear key.
- `Nats-Linear-PQC-Public-Key` carries the ephemeral ML-KEM/Kyber public key material.
- `DPoP` carries a sender proof token for the linear publish.

## Security

The Go helper can establish NATS connections with a supplied mTLS `tls.Config`, generate ephemeral ML-KEM-768 (Kyber) public keys per linear publish, and attach ES256 DPoP proofs. Rust and TS/JS helpers expose the same security headers for callers that supply DPoP tokens and PQC public key material from their runtime security layer.

## Runtime behavior

When a received linear message is accessed through the helper API, the helper returns the payload and immediately destroys its retained payload reference. If the TTL expires before access, the helper destroys the retained payload and future access returns no payload.

If `Nats-Event-Type` is not set to `Linear`, server fanout and repeated helper access keep the existing behavior.

## Managed queue lifecycle

The Go helper includes `StartLinearQueue`, which keeps a queue subscription open until a configured destroy subject receives a NATS event. If the connection drops and no destroy event arrives, the NATS client reconnects every second by default until `ReconnectFor` is exhausted.

## Outbox and DLQ

The outbox stores pending linear publishes in memory, assigns an outbox id, and retries them when `Flush`/`flush` is called. If an entry reaches the configured max attempts and a DLQ subject is configured, the helper publishes the original payload to that DLQ subject with diagnostic headers.

For crash-safe producer guarantees, persist outbox entries in application storage or use JetStream; these helpers are intentionally lightweight and process-local.

## TypeScript transport mode

The TypeScript helper exposes `connectLinear({ mode })` so callers can keep the same NATS command surface while changing the transport:

```ts
import { connectLinear } from "nats-linear-client";

const nc = await connectLinear({
  mode: "QUIC",
  servers: "nats+quic://localhost:4222",
  quic: { path: "/nats" },
});

nc.publish("linear.example", new TextEncoder().encode("payload"));
```

`mode: "TCP"` delegates to the regular `nats` Node transport. `mode: "QUIC"` installs the QUIC adapter from `clients/ts-js/src/adapters/quic.ts`, accepts the server-opened reliable WebTransport bidirectional stream, and passes raw NATS protocol frames (`CONNECT`, `PING`, `SUB`, `PUB`/`HPUB`, etc.) through that stream so publish and subscribe calls keep the same command names and semantics. Runtimes without a global WebTransport implementation can pass one through `quic.webTransport`; constructor-specific options can be passed through `quic.webTransportOptions`.

Server-side QUIC is enabled with the `quic` configuration block. It listens on UDP, requires TLS for HTTP/3/WebTransport, and hands each WebTransport stream to the same NATS client parser used by TCP:

```hcl
quic {
  listen: "127.0.0.1:4223"
  path: "/nats"
  tls {
    cert_file: "server.pem"
    key_file: "key.pem"
  }
}
```

## Implementations

- Go: `clients/go/linear.go`
- Rust: `clients/rust/src/lib.rs`
- TypeScript/JavaScript: `clients/ts-js/src/index.ts`
- TypeScript/JavaScript QUIC adapter: `clients/ts-js/src/adapters/quic.ts`
- Gleam: `clients/gleam/src/nats_linear.gleam`

The language-neutral AI generation contract lives in `docs/linear-client-definition.json`. For a full architecture and operations guide, see `docs/linear-nats-architecture.md`.
