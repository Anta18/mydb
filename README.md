# Engine Database

This repository contains an experimental SQL database engine written in Rust.

## Building

```bash
cargo build --manifest-path engine/Cargo.toml
```

## Running the server

Start the HTTP server with:

```bash
cargo run --manifest-path engine/Cargo.toml -- server
```

The server listens on `127.0.0.1:3000` by default and persists data to `data.db` with a write-ahead log in `wal.log`.

## Using the CLI shell

In another terminal, run:

```bash
cargo run --manifest-path engine/Cargo.toml -- shell
```

The shell prompts and lets you submit SQL statements ending with `;`.

## Running tests

```bash
cargo test --manifest-path engine/Cargo.toml
```

These unit tests cover the lower level storage components like the buffer pool and page file.
