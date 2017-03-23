FROM jimmycuadra/rust:1.16.0

RUN mkdir -p /rust/app
WORKDIR /rust/app

ENV TARGET=x86_64-unknown-linux-gnu

COPY . /rust/app
RUN cargo build --release --target=$TARGET
RUN cp ./target/$TARGET/release/linkerd-tcp /usr/local/bin

ENTRYPOINT ["/usr/local/bin/linkerd-tcp"]
