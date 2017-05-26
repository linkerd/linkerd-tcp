RUST_VERSION = 1.16.0
DOCKER_IMAGE = alexcrichton/rust-slave-dist:2015-10-20b
RUST_TARBALL = rust-$(RUST_VERSION)-x86_64-unknown-linux-gnu.tar.gz
RUST_SOURCE = https://static.rust-lang.org/dist/$(RUST_TARBALL)

ifeq ($(shell uname -s),Darwin)
all: linux darwin

darwin: target/x86_64-apple-darwin/release/linkerd-tcp

target/x86_64-apple-darwin/release/linkerd-tcp:
	cargo build --release --verbose --target=x86_64-apple-darwin

else
all: linux
endif

linux: $(RUST_TARBALL) target/x86_64-unknown-linux-gnu/release/linkerd-tcp

$(RUST_TARBALL):
	curl -L -o $(RUST_TARBALL) $(RUST_SOURCE)

target/x86_64-unknown-linux-gnu/release/linkerd-tcp: $(RUST_TARBALL)
	docker pull $(DOCKER_IMAGE)
	@docker run \
    --rm -v $(shell pwd):/rust/app \
    -u root \
    -w /rust/app \
    --entrypoint=/bin/bash \
    $(DOCKER_IMAGE) \
    -exc "cd /tmp && \
    (gzip -dc /rust/app/$(RUST_TARBALL) | tar xf -) && \
    ./rust-$(RUST_VERSION)-x86_64-unknown-linux-gnu/install.sh --without=rust-docs && \
    cd /rust/app && \
    cargo build --release --verbose --target=x86_64-unknown-linux-gnu"

clean:
	-rm -rf target

distclean: clean
	-rm -f $(RUST_TARBALL)
