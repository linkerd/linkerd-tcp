# linkerd.tcp #

A native TCP proxy for the linkerd service mesh.

Status: _experimental_

## Features ##

- Resolves destinations through namerd for generic, dynamic service
  discovery
- Least-connection layer 4 load balancing
- Supports endpoint weighting (i.e. for "red line" testing)

### TODO ###

- [] Stream updates from namerd. (Currently polling).
- [] Circuit breaking.
- [] Initiate downstream connections ahead-of-time to reduce
  connection establishment latency.
- [] Metrics
- [] Config file (read linkerd's config?)
- [] Native TLS upstream
- [] Native TLS downstream
- [] TLS Key rotation

## Quickstart ##

1. Install [Rust and Cargo][install-rust].
2. Configure and run [namerd][namerd] with the _io.l5d.httpController_ interface.
3. From this repository, run: `cargo run -- -i 10 -n
   namerd.example.com:4180 -N mynamespace /svc/targetsvc`

## Usage ##



### Logging ###

```
tcpd 0.0.1
Steve Jenson <stevej@buoyant.io>
Oliver Gould <ver@buoyant.io>
A native TCP proxy for the linkerd service mesh

USAGE:
    tcpd [OPTIONS] <TARGET>

FLAGS:
    -h, --help       Prints help information
    -V, --version    Prints version information

OPTIONS:
    -l, --listen-addr <ADDR>        Accept connections on the given local address and port [default: 0.0.0.0:7575]
    -n, --namerd-addr <ADDR>        The address of namerd's HTTP interface [default: 127.0.0.1:4180]
    -i, --namerd-interval <SECS>    Namerd refresh interval in seconds [default: 60]
    -N, --namerd-ns <NS>            Namerd namespace in which the target will be resolved [default: default]

ARGS:
    <TARGET>    Destination name (e.g. /svc/foo)
```

As in all rust applications, the `RUST_LOG` environment variable.


## License ##

Copyright 2017, Buoyant Inc. All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use these files except in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.


<!-- references -->
[install-rust]: https://www.rust-lang.org/en-US/install.html
[namerd]: https://github.com/linkerd/linkerd/tree/master/namerd
