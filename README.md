# linkerd-tcp #

A native TCP proxy for the linkerd service mesh.

Status: _experimental_

## Features ##

- Resolves destinations through namerd for generic, dynamic service
  discovery
- Least-connection layer 4 load balancing
- Supports endpoint weighting (i.e. for "red line" testing)

### TODO ###

- [ ] Metrics (stevej)
- [ ] Admin: export prom (stevej)
- [ ] Stream updates from namerd. (Currently polling).
- [ ] Config file
- [ ] Native TLS upstream
- [ ] Native TLS downstream

#### Later ####

- [ ] Configurable circuit breaking.
- [ ] Configurable connection management.
- [ ] TLS Key rotation

## Quickstart ##

1. Install [Rust and Cargo][install-rust].
2. Configure and run [namerd][namerd] with the _io.l5d.httpController_ interface.
3. From this repository, run: `cargo run -- -i 10 -n
   namerd.example.com:4180 -N mynamespace /svc/targetsvc`

## Usage ##



```
linkerd-tcp 0.0.1
A native TCP proxy for the linkerd service mesh

USAGE:
    linkerd-tcp [OPTIONS] <TARGET>

FLAGS:
    -h, --help       Prints help information
    -V, --version    Prints version information

OPTIONS:
    -l, --listen-addr <ADDR>        Accept connections on the given local address and port [default: 0.0.0.0:7575]
    -n, --namerd-addr <ADDR>        The address of namerd's HTTP interface [default: 127.0.0.1:4180]
    -i, --namerd-interval <SECS>    Namerd refresh interval in seconds [default: 60]
    -N, --namerd-ns <NS>            Namerd namespace in which the target will be resolved [default: default]
    -w, --window-size-kb <KB>        [default: 64]

ARGS:
    <TARGET>    Destination name (e.g. /svc/foo) to be resolved through namerd
```


### Logging ###

As in most rust applications, the `RUST_LOG` environment variable.


## License ##

Copyright 2017, Buoyant Inc. All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use these files except in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.


<!-- references -->
[install-rust]: https://www.rust-lang.org/en-US/install.html
[namerd]: https://github.com/linkerd/linkerd/tree/master/namerd
