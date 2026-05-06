# Linear NATS clients

These helper clients use NATS headers to mark an event as `Linear`:

- `Nats-Event-Type: Linear` enables linear delivery on this server build.
- `Nats-Linear-TTL: <duration>` is forwarded to clients so they can destroy an unread payload after a time-to-live.

When a received linear message is accessed through the helper API, the helper returns a copy/view of the payload and immediately destroys its retained payload reference. If the TTL expires before access, the helper destroys the retained payload and future access returns no payload.

If `Nats-Event-Type` is not set to `Linear`, server fanout behavior is unchanged.

## Implementations

- Go: `clients/go/linear.go`
- Rust: `clients/rust/src/lib.rs`
- TypeScript/JavaScript: `clients/ts-js/src/index.ts`
