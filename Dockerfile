FROM debian:jessie-slim

COPY ./target/x86_64-unknown-linux-gnu/release/linkerd-tcp /usr/local/bin/

ENTRYPOINT ["/usr/local/bin/linkerd-tcp"]
