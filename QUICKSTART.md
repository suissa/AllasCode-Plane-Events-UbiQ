# Quickstart: Linear NATS Server

This quickstart gets the Linear NATS server and helper clients ready from a fresh checkout.

## 1. Prerequisites

Required for the server:

- Go 1.25+
- npm, for JavaScript tooling and the helper scripts

Optional, only if you want to run every client test locally:

- Rust/Cargo for `clients/rust`
- Gleam for `clients/gleam`

The `ubiq setup` command checks these tools. It installs/fetches project dependencies it can manage directly and prints a warning for optional toolchains that are not installed.

## 2. Setup

From the repository root, run:

```bash
./ubiq setup
```

If you want to call the command as `ubiq setup` without `./`, add this repository to your `PATH` first:

```bash
export PATH="$PWD:$PATH"
ubiq setup
```

What setup does:

1. Downloads Go modules.
2. Builds the server into `bin/nats-server`.
3. Installs TypeScript/JavaScript client dependencies.
4. Installs Gleam client npm metadata and fetches Gleam dependencies when the `gleam` binary is available.
5. Fetches Rust client dependencies when `cargo` is available.

## 3. Run the server

Run with defaults (`127.0.0.1:4222`):

```bash
./ubiq run
```

Or, if the repo is on your `PATH`:

```bash
ubiq run
```

Pass native NATS server flags after `run`:

```bash
./ubiq run -DV -p 4223
```

You can also change the default host/port used when no explicit flags are supplied:

```bash
UBIQ_HOST=0.0.0.0 UBIQ_PORT=4223 ./ubiq run
```

## 4. Smoke test with a normal NATS client

In another terminal, use any NATS client to connect to the server at `nats://127.0.0.1:4222`.

Linear messages are normal NATS header messages with:

```text
Nats-Event-Type: Linear
```

Unmarked messages keep normal NATS fanout behavior.

## 5. Run tests

Core/server linear behavior:

```bash
go test ./server -run 'TestLinearEvent|TestNonLinearEvent'
```

Go helper client:

```bash
go test ./clients/go
```

TypeScript/JavaScript helper client:

```bash
npm install --prefix clients/ts-js
cd clients/ts-js && npx tsc && node --test test/index.test.mjs
```

Rust helper client, if Cargo is installed:

```bash
cargo test --manifest-path clients/rust/Cargo.toml
```

Gleam helper client, if Gleam is installed:

```bash
npm --prefix clients/gleam test
```

## 6. AI client generation contract

Use `docs/linear-client-definition.json` to generate equivalent clients in new languages. The JSON defines:

- the required behaviors instead of language-specific data types;
- a specific prompt for AI generation;
- the TDD workflow;
- mandatory tests;
- BDD scenarios that unify the full feature set.
