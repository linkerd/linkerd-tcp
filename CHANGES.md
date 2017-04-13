## In the next release

## 0.0.3

* New admin endpoint! POST to `/shutdown` to tell the process to exit.
* Fix connection leak when one peer disconnected.
* Fix stat reporting issue for the rx bytes metric.
* Improve error message when config file cannot be read.
* Update examples to bind to all available sockets by default.
* Add guidelines for contributing to the linkerd-tcp repo.
* Add script for building a slimmer docker image.
* **Breaking change**: Convert namerd config to use `url` field instead of `addr`.
* **Breaking change**: Convert server config attributes from camel to snake case.

## 0.0.2

* Expose a configurable admin server with Prometheus metrics.

## 0.0.1

Introducing linkerd-tcp. :balloon:

* Run a TCP load balancer for the [Linkerd][https://linkerd.io] service mesh.
* Support TLS and integrate with Namerd for service discovery.
* Read a YAML or JSON configuration file at startup.
