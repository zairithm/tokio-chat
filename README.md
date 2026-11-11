# tokio-chat

[![CI](https://github.com/zairithm/tokio-chat/actions/workflows/ci.yml/badge.svg)](https://github.com/zairithm/tokio-chat/actions/workflows/ci.yml)

A chat server where many clients talk at once over TCP, built with async Rust and Tokio.

## What this project covers

- Async I/O with `tokio` and spawned tasks
- Sharing state between tasks with channels
- Integration tests that start a real server on a random port

## Getting started

```bash
cargo run
# in two other terminals:
nc 127.0.0.1 8080
```

## Roadmap

Each open item is tracked as a [GitHub issue](https://github.com/zairithm/tokio-chat/issues).

- [x] Accept many clients and echo their lines
- [ ] Broadcast every message to all clients
- [ ] Nicknames and join/leave messages
- [ ] Commands: `/nick`, `/who`, `/quit`
- [ ] Graceful shutdown on Ctrl-C
- [ ] A small client binary

## License

MIT
