# linkerd-tcp #

A TCP load balancer for the [linkerd][linkerd] service mesh.

Status: _beta_

[![CircleCI](https://circleci.com/gh/linkerd/linkerd-tcp/tree/master.svg?style=svg)](https://circleci.com/gh/linkerd/linkerd-tcp/tree/master)

## Features ##

- Lightweight, native **TCP** and **TLS** load balancer built on [tokio].
  - Weighted-least-loaded [P2C][p2c] load balancing.
  - Minimal resource utilization: typically <.5 cores with ~2MB RSS.
- Tightly integrated with the [linkerd service mesh][namerd].
  - Supports endpoint weighting (i.e. for "red line" testing).
- Modern Transport Layer Security via [rustls][rustls]:
  - TLS1.2 and TLS1.3 (draft 18) only.
  - ECDSA or RSA server authentication by clients.
  - RSA server authentication by servers.
  - Forward secrecy using ECDHE; with curve25519, nistp256 or nistp384 curves.
  - AES128-GCM and AES256-GCM bulk encryption, with safe nonces.
  - Chacha20Poly1305 bulk encryption.
  - ALPN support.
  - SNI support.

## Quickstart ##

1. Install [Rust and Cargo][install-rust].
2. Run [namerd][namerd].  `./namerd.sh` fetches, configures, and runs namerd using a local-fs-backed discovery (in ./tmp.discovery).
3. From this repository, run: `cargo run -- example.yml`

We :heart: pull requests! See [CONTRIBUTING.md](CONTRIBUTING.md) for info on
contributing changes.

## Usage ##

```
linkerd-tcp 0.1.0
A native TCP proxy for the linkerd service mesh

USAGE:
    linkerd-tcp <PATH>

FLAGS:
    -h, --help       Prints help information
    -V, --version    Prints version information

ARGS:
    <PATH>    Config file path
```

### Example configuration ###

```yaml

# Administrative control endpoints are exposed on a dedicated HTTP server. Endpoints
# include:
# - /metrics -- produces a snapshot of metrics formatted for prometheus.
# - /shutdown -- POSTing to this endpoint initiates graceful shutdown.
# - /abort -- POSTing to this terminates the process immediately.
admin:
  port: 9989

  # By default, the admin server listens only on localhost. We can force it to bind
  # on all interfaces by overriding the IP.
  ip: 0.0.0.0

  # Metrics are snapshot at a fixed interval of 10s.
  metricsIntervalSecs: 10

# A process exposes one or more 'routers'. Routers connect server traffic to
# load balancers.
routers:

  # Each router has a 'label' for reporting purposes.
  - label: default

    # Each router is configured to resolve names.
    # Currently, only namerd's HTTP interface is supported:
    interpreter:
      kind: io.l5d.namerd.http
      baseUrl: http://localhost:4180
      namespace: default
      periodSecs: 20

    servers:

      # Each router has one or more 'servers' listening for incoming connections.
      # By default, routers listen on localhost. You need to specify a port.
      - port: 7474
        dstName: /svc/default
        # You can limit the amount of time that a server will wait to obtain a
        # connection from the router.
        connectTimeoutMs: 500

      # By default each server listens on 'localhost' to avoid exposing an open
      # relay by default. Servers may be configured to listen on a specific local
      # address or all local addresses (0.0.0.0).
      - port: 7575
        ip: 0.0.0.0
        # Note that each server may route to a different destination through a
        # single router:
        dstName: /svc/google
        # Servers may be configured to perform a TLS handshake.
        tls:
          defaultIdentity:
            privateKey: private.pem
            certs:
              - cert.pem
              - ../eg-ca/ca/intermediate/certs/ca-chain.cert.pem

    # Clients may also be configured to perform a TLS handshake.
    client:
      kind: io.l5d.static
      # We can also apply linkerd-style per-client configuration:
      configs:
        - prefix: /svc/google
          connectTimeoutMs: 400
          # Require that the downstream connection be TLS'd, with a
          # `subjectAltName` including the DNS name _www.google.com_
          # using either our local CA or the host's default openssl
          # certificate.
          tls:
            dnsName: "www.google.com"
            trustCerts:
              - ../eg-ca/ca/intermediate/certs/ca-chain.cert.pem
              - /usr/local/etc/openssl/cert.pem
```

### Logging ###

Logging may be enabled by setting `RUST_LOG=linkerd_tcp=info` on the environment.  When
debugging, set `RUST_LOG=trace`.

## Docker ##

To build the  linkerd/linkerd-tcp docker image, run:

```bash
./dockerize latest
```

Replace `latest` with the version that you want to build.

Try running the image with:

```bash
docker run -v `pwd`/example.yml:/example.yml linkerd/linkerd-tcp:latest /example.yml
```

## Code of Conduct ##

This project is for everyone. We ask that our users and contributors take a few minutes to
review our [code of conduct][coc].

## License ##

Copyright 2017-2018 Linkerd-TCP authors. All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use these files except in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.

<!-- references -->
[coc]: https://github.com/linkerd/linkerd/wiki/Linkerd-code-of-conduct
[install-rust]: https://www.rust-lang.org/en-US/install.html
[linkerd]: https://github.com/linkerd/linkerd
[namerd]: https://github.com/linkerd/linkerd/tree/master/namerd
[p2c]: https://www.eecs.harvard.edu/~michaelm/postscripts/mythesis.pdf
[rustls]: https://github.com/ctz/rustls
[tokio]: https://tokio.rs
