# UbiQ

UbiQ is a NATS server with super powers

[NATS documentation](https://docs.nats.io)

UbiQ extends the NATS server with an opt-in Linear event layer, multi-language helper clients, outbox/DLQ workflows, optional security metadata, and developer automation. It keeps regular NATS behavior intact unless a publisher explicitly marks a message as Linear.

## What UbiQ adds to NATS

### Header-driven Linear delivery

UbiQ introduces a Linear event mode using NATS headers:

```text
Nats-Event-Type: Linear
```

When this header is present, the server delivers the message to exactly one matching subscriber instead of fanning it out to every plain subscriber. Messages without this header keep normal NATS fanout behavior.

### Client-side single-access payloads

UbiQ helper clients wrap received messages with Linear-aware access semantics:

- the first access to a Linear payload returns the payload;
- after the first access, the helper destroys its retained local copy;
- a second access to the same Linear payload returns no payload;
- non-Linear messages remain reusable and preserve normal client behavior.

### TTL-based local retention

Linear messages can include a local retention TTL:

```text
Nats-Linear-TTL: <milliseconds>
```

If a Linear payload is not accessed before the TTL expires, the helper client destroys its retained local copy. This TTL is a client-side memory-retention policy, not broker-side persistence or JetStream retention.

### Producer outbox

UbiQ clients include an in-memory outbox pattern for Linear publishes:

1. enqueue a Linear publish locally;
2. copy the payload into the outbox;
3. assign a correlatable outbox id;
4. flush pending entries;
5. remove entries after successful publish;
6. keep failed entries for retry until the configured attempt limit.

Outbox entries carry:

```text
Nats-Linear-Outbox-Id: <id>
```

### Dead-letter queue (DLQ)

When a publish reaches the configured retry limit, the helper can move the payload to a DLQ subject with diagnostic headers:

```text
Nats-Linear-DLQ-Reason: <error>
Nats-Linear-Original-Subject: <original subject>
Nats-Linear-Outbox-Id: <id>
```

The DLQ preserves the original payload and enough metadata for operators to inspect, replay, or discard failed events.

### Optional security metadata

UbiQ supports optional security evidence on Linear publishes:

- mTLS through the native NATS/TLS configuration path;
- PQC/Kyber metadata using ML-KEM-768 public key evidence;
- DPoP proof metadata attached to the publish.

Security-aware consumers or gateways can validate these headers before processing a payload:

```text
Nats-Linear-PQC-Alg: ML-KEM-768
Nats-Linear-PQC-Public-Key: <ephemeral public key evidence>
DPoP: <proof token>
```

### Managed Linear queues

The Go helper includes a managed queue lifecycle helper. A managed Linear queue:

- remains open after creation;
- closes only when a configured NATS destroy subject receives an event, or when the local context is canceled;
- reconnects every second by default when the connection drops;
- keeps reconnecting until the configured reconnect window is exhausted.

This is useful for long-lived Linear workers that should not disappear just because the initial setup has completed.

## Multi-language clients

UbiQ includes helper clients for:

- Go: `clients/go`
- Rust: `clients/rust`
- TypeScript/JavaScript: `clients/ts-js`
- Gleam: `clients/gleam`

The clients follow the same behavior contract while using language-appropriate APIs and test tools.

## AI client generation contract

`docs/linear-client-definition.json` defines the Linear client behavior in a language-neutral way. It includes:

- the exact behaviors a generated client must implement;
- a specific prompt for AI-based client generation;
- a TDD workflow requirement;
- mandatory tests;
- BDD scenarios that unify Linear delivery, TTL, outbox, DLQ, security metadata, and managed queues.

Use this JSON to generate equivalent clients for additional languages without copying type definitions from existing implementations.

## Quickstart

Use the `ubiq` helper to set up dependencies and run the server.

```bash
./ubiq setup
./ubiq run
```

If you add this repository to your `PATH`, you can run:

```bash
ubiq setup
ubiq run
```

See [QUICKSTART.md](./QUICKSTART.md) for prerequisites, setup details, run examples, smoke tests, and client test commands.

## Running the server

By default, `ubiq run` starts the server on `127.0.0.1:4222`:

```bash
./ubiq run
```

Pass native NATS server flags after `run`:

```bash
./ubiq run -DV -p 4223
```

Or configure the default host and port:

```bash
UBIQ_HOST=0.0.0.0 UBIQ_PORT=4223 ./ubiq run
```

## Documentation map

- [Quickstart](./QUICKSTART.md)
- [Linear client behavior definition](./docs/linear-client-definition.json)
- [Linear NATS architecture](./docs/linear-nats-architecture.md)
- [Client helpers](./clients/README.md)
- [NATS documentation](https://docs.nats.io)

## Compatibility

UbiQ remains compatible with normal NATS usage:

- existing NATS clients can publish and subscribe normally;
- unmarked messages preserve standard NATS fanout;
- Linear behavior activates only when the publisher sets `Nats-Event-Type: Linear`;
- security metadata is additive and can be ignored by consumers that do not validate it.

## License

Unless otherwise noted, the NATS source files are distributed under the Apache Version 2.0 license found in the LICENSE file.
