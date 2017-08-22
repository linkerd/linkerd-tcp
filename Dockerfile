FROM linkerd/rustup-nightly:v1
RUN rustup update nightly
RUN mkdir -p /linkerd-tcp
COPY . /linkerd-tcp
WORKDIR /linkerd-tcp
RUN cargo build --release

FROM debian:jessie-slim
COPY --from=0 /linkerd-tcp/target/x86_64-unknown-linux-gnu/release/linkerd-tcp /usr/local/bin/
ENTRYPOINT ["/usr/local/bin/linkerd-tcp"]
