# Galadriel is an experiment in building TCP middleware in Rust

Quick howto

## Building and Running

Mac/Linux

* Install Rust with `curl -sSf https://static.rust-lang.org/rustup.sh | sh`
* `cargo build` (or `cargo build --release` if you're ready for production)
* `target/debug/proxy2` or `target/release/proxy2`

## FAQ

### Usage

`target/release/proxy2 source_ip:port target_ip:port`

Yes, you have to use IP addresses for now, we don't resolve names.

### Logging

`RUST_LOG=debug target/release/proxy2` will show you tons of tokio logging messages.



# License

This is copyright Buoyant, Inc and released under the terms of the Apache License 2.0