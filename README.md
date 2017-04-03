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
2. Configure and run [namerd][namerd].
3. From this repository, run: `cargo run -- example.yml`

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
proxies:
  - label: default
    servers:
      # Listen on two ports, one using a self-signed TLS certificate.
      - kind: io.l5d.tcp
        addr: 0.0.0.0:7474
      - kind: io.l5d.tls
        addr: 0.0.0.0:7575
        defaultIdentity:
          privateKey: private.pem
          certs:
            - cert.pem
            - ../eg-ca/ca/intermediate/certs/ca-chain.cert.pem

    # Lookup /svc/google in namerd.
    namerd:
      addr: 127.0.0.1:4180
      path: /svc/google

    # Require that the downstream connection be TLS'd, with a `subjectAltName` including
    # the DNS name _www.google.com_ using either our local CA or the host's default
    # openssl certificate.
    client:
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

To build the linkerd/linkerd-tcp docker image, run:

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

Copyright 2017, Buoyant Inc. All rights reserved.

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
