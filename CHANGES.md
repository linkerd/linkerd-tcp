## In the next release

## 0.1.1
* Trace negotiated SNI & ALPN at the end of the server handshake.
* Improve handling and reporting of TLS configuration errors.
* Update to latest versions of dependencies.
* Switch the official Rust base docker image.
* Remove use of deprecated downstream API members.
* Remove dependency on nightly Rust to build linkerd-tcp.

## 0.1.0
linkerd-tcp 0.1.0 constitutes a major rewrite.

Previously, linkerd-tcp did not properly utilize tokio's task model, which lead
to a number of performance and correctness problems. Furthermore, linkerd-tcp's
configuration interface was substantially different from linkerd's, which
caused some confusion.

* Significant performance and correctness improvements.
* Add support for connection and stream timeouts.
* Add several additional metrics.
* **Breaking change**: Change configuration file syntax to be structured like a Linkerd router:
  * Rename `proxies` key to `routers`
  * Change `servers` to take separate `ip` and `port` keys, rather than `addr`.
  * Change `namerd` section in server configuration to Linkerd-style `interpreter`
    section on a router configuration. The `path` key is now specified per-server as `dstPath`.

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