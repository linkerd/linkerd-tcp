# linkerd-tcp #

A native TCP proxy for the linkerd service mesh.

Status: _experimental_

## Features ##

- Resolves destinations through namerd for generic, dynamic service
  discovery
- Least-connection layer 4 load balancing
- Supports endpoint weighting (i.e. for "red line" testing)
- Modern, flexible, & secure Transport Layer Security.

### TODO ###

- [ ] Metrics (stevej)
- [ ] Admin: export prom (stevej)
- [ ] Configurable circuit breaking
- [ ] Configurable connection management
- TLS
  - [ ] Key rotation
  - [ ] Mutual authentication
  - [ ] ALPN pass-thru
- [ ] Use namerd's streaming/long-polling interfaces
- [ ] Use `bytes`

## Quickstart ##

1. Install [Rust and Cargo][install-rust].
2. Configure and run [namerd][namerd] with the _io.l5d.httpController_ interface.
3. From this repository, run: `cargo run -- example.yml`

## Usage ##

```
linkerd-tcp 0.0.1
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
  - servers:
      # Listen on two ports, one using a self-signed TLS certificate.
      - kind: io.l5d.tcp
        addr: 127.0.0.1:7474
      - kind: io.l5d.tls
        addr: 0.0.0.0:7575
        defaultIdentity:
          privateKeyPath: private.pem
          certPaths:
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
        name: "www.google.com"
        trustCertPaths:
          - ../eg-ca/ca/intermediate/certs/ca-chain.cert.pem
          - /usr/local/etc/openssl/cert.pem
```

### Logging ###

Logging may be enabled by setting `RUST_LOG=linkerd_tcp=info` on the environment.  When
debugging, set `RUST_LOG=trace`.

## License ##

Copyright 2017, Buoyant Inc. All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use these files except in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.

<!-- references -->
[install-rust]: https://www.rust-lang.org/en-US/install.html
[namerd]: https://github.com/linkerd/linkerd/tree/master/namerd
