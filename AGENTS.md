# mineshaft - Agent Guide

Mineshaft extends Minecraft Bedrock LAN play over the internet. It speaks
NetherNet (WebRTC) LAN discovery to the local game, advertises remote worlds
into the Friends tab, and shuttles WebRTC signaling between the two games
through an always-on rendezvous server. The games establish the WebRTC data
channel directly with each other; mineshaft never proxies game traffic.

## Repository layout

```
crates/
  core/                 mineshaft-core: shared runtime (this is the real library)
    src/config.rs       ServerAdvertisement, DiscoveryConfig, RelayConfig
    src/discovery.rs    Advertises ServerData v6 into the local game via UDP 7551
    src/net.rs          Rendezvous wire protocol (4-byte LE length + JSON over TCP)
    src/platform.rs     Glob re-export of the one platform backend for this target
    src/relay.rs        Host registry / relay bookkeeping (see "Dead code" below)
    src/session.rs      NetherNet SessionRegistry
    src/transport.rs    Thin wrapper over nethernet-tokio (Listener/Transport)
  platform/
    linux/              /proc/net/udp probing, /proc/*/comm process scan
    android/            Same /proc strategy + VPN socket-protect hook
    windows/            GetExtendedUdpTable / ToolHelp (stubs off-Windows)
  nethernet/            git submodule - vendored NetherNet stack
  raknet/               git submodule - excluded from the workspace
examples/
  client/               The 24/7 node daemon (NOT a toy - the actual product)
  server/               The rendezvous server (also the actual product)
```

Platform crates are selected at compile time: `core/Cargo.toml` depends on
exactly one of them per `cfg(target_os = ...)`, and `core::platform`
re-exports it wholesale. Exactly one backend must compile per target.

## Build and test

```
git submodule update --init   # nethernet + raknet (nethernet is required)
cargo check --workspace       # fast sanity check (raknet is excluded)
cargo clippy --workspace      # currently: a handful of minor warnings
cargo test --workspace        # almost no coverage today - see below
```

Toolchain is pinned (`rust-toolchain.toml`, 1.98.1, edition 2024, resolver 3).
Cross-checking the Windows backend from Linux needs
`cargo check -p mineshaft-windows --target x86_64-pc-windows-msvc`.

## Protocol constants that matter

- UDP **7551** is the game's LAN discovery socket. The game owns it; we
  advertise *to* it from an ephemeral socket instead of binding it.
- Advertisements are NetherNet discovery `ResponsePacket`s (ServerData v6).
- Signals (`CONNECTREQUEST`, `CONNECTRESPONSE`, `CANDIDATEADD`) are forwarded
  as raw text lines inside JSON frames; `target_sender_id == 0` is the magic
  sentinel meaning "route via the session map, host -> joiner".
- The client probes whether the game is hosting by sending a real discovery
  request to 127.0.0.1:7551 / [::1]:7551 and waiting 250 ms.
