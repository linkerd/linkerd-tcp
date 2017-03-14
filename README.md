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
- [ ] Native TLS upstream (ver)
- [ ] Native TLS downstream (ver)

#### Later ####

- [ ] Configurable circuit breaking.
- [ ] Configurable connection management.
- [ ] TLS Key rotation
- [ ] Use one of namerd's streaming/long-polling interfaces.

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
    linkerd-tcp <PATH>

FLAGS:
    -h, --help       Prints help information
    -V, --version    Prints version information

ARGS:
    <PATH>    Config file path
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
