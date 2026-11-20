# tokio-chat

[![CI](https://github.com/zairithm/tokio-chat/actions/workflows/ci.yml/badge.svg)](https://github.com/zairithm/tokio-chat/actions/workflows/ci.yml)

A multi-user chat server over TCP, written in async Rust with Tokio.

## Demo

Start the server, then connect from two terminals with `nc` (or `telnet`):

```console
$ cargo run
listening on 127.0.0.1:8080 (Ctrl-C to stop)
```

```console
$ nc 127.0.0.1 8080                      $ nc 127.0.0.1 8080
Welcome to tokio-chat!                   Welcome to tokio-chat!
Choose a nickname:                       Choose a nickname:
alice                                    bob
Hi alice! Commands: /nick <name>, ...    Hi bob! Commands: /nick <name>, ...
* bob joined                             alice: hello bob
hello bob                                /who
bob: hi!                                 2 online: alice, bob
                                         hi!
```

Pressing Ctrl-C on the server tells every client `Server is shutting down. Bye!` and closes cleanly.

## Features

| | |
|---|---|
| Broadcast | Every line goes to all other clients as `nickname: text` |
| Nicknames | Asked on connect; unique (case-insensitive), no spaces, up to 20 characters |
| Announcements | `* name joined`, `* name left`, `* old is now new` |
| Commands | `/nick <name>`, `/who`, `/help`, `/quit` |
| Shutdown | Ctrl-C stops accepting, says goodbye to everyone and waits for them to disconnect |
| Slow clients | A client that falls behind is told how many messages it missed |

## How it works

```
                accept loop  (run_until)
                    │  spawns one task per client into a JoinSet
       ┌────────────┼────────────┐
   client task  client task  client task
       │            │            │
       └──── broadcast channel ──┘    every task sends into it and has its own receiver
       └──── Arc<Mutex<users>> ──┘    nicknames in use, locked only briefly
       └──── watch channel ──────┘    flips to true on shutdown
```

Each client task runs a `tokio::select!` loop over three things at once: a line from its socket, a message from the broadcast channel, and the shutdown signal. A client subscribes to the broadcast channel only after choosing a nickname, so it gets no backlog and unnamed connections hear nothing.

Parsing (`parse` into an `Input` enum) and nickname rules (`validate_nick`) are plain functions with unit tests. Integration tests in `tests/chat.rs` start a real server on a random port and drive it with several TCP clients.

## Development

```bash
cargo test
cargo clippy --all-targets -- -D warnings
cargo fmt
```

## Roadmap

- [x] Accept many clients
- [x] Broadcast every message to all clients
- [x] Nicknames and join/leave messages
- [x] Commands: `/nick`, `/who`, `/help`, `/quit`
- [x] Graceful shutdown on Ctrl-C
- [ ] A small client binary with a nicer prompt
- [ ] Rooms (`/join <room>`)
- [ ] Private messages (`/msg <name> <text>`)

## License

MIT
