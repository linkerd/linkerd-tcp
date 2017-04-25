# Rust Stream Balancer Design

## Prototype

The initial implementation is basically a prototype. It proves the concept, but it has
severe deficiencies that cause performance (and probably correctness) problems.
Specifically, it implements its own polling... poorly.

At startup, the configuration is parsed. For each **proxy**, the namerd and serving
configurations are split and connectd by an async channel so that namerd updates are
processed outside of the serving thread. All of the namerd watchers are collected to be
run together with the admin server. Once all of the proxy configurations are processed,
the application is run.

The admin thread is started, initiating all namerd polling and starting the admin server.

Simultaneously, all of the proxies are run in the main thread. For each of these, a
**connector** is created to determine how all downstream connections are established for
the proxy. A **balancer** is created with the connector and a stream of namerd updates. An
**acceptor** is created for each listening interface, which manifests as a stream of
connections, connections. The balancer is made shareable across servers by creating an
async channel and each server's connections are streamed into a sink clone. The balancer
is driven to process all of these connections.

The balancer implements a Sink that manages _all_ I/O and connection management. Each
time `Balancer::start_send` or `Balancer::poll_complete` is called, the following work is
done:
- _all_ conneciton streams are checked for I/O and data is transfered;
- closed connections are reaped;
- service discovery is checked for updates;
- new connections are established;
- stats are recorded;

## Lessons/Problems

### Inflexible

This model doesn't really reflect that of linkerd. We have no mechanism to _route_
connections. All connections are simply forwarded. We cannot, for instance, route based on
client credentials or SNI destination.

### Inefficient

Currently, each balancer is effectively a scheduler, and a pretty poor one at that. I/O
processing should be far more granular and we shouldn't update load balancer endpoints in
the I/O path (unless absolutely necessary).

### Timeouts

We need several types of timeouts that are not currently implemented:
- Connection timeout: time from incoming connection to outbound established.
- Stream lifetime: maximum time a stream may stay open.
- Idle timeout: maximum time a connection may stay open without transmitting data.

## Proposal

linkerd-tcp should become a _stream router_. In the same way that linkerd routes requests,
linkerd-tcp should route connections. The following is a rough, evolving sketch of how
linkerd-tcp should be refactored to accomodate this:

The linkerd-tcp configuration should support one or more **routers**. Each router is
configured with one or more **servers**. A server, which may or may not terminate TLS,
produces a stream of incoming connections comprising an envelope--a source identity (an
address, but maybe more) and a destination name--and a bidirectional data stream. The
server may choose the destination by static configuration or as some function of the
connection (e.g. client credentials, SNI, etc). Each connection envelope may be annotated
with a standard set of metadata including, for example, an optional connect deadline,
stream deadline, etc.

The streams of all incoming connections for a router are merged into a single stream of
enveloped connections. This stream is forwarded to a **binder**. A binder is responsible
for maintaining a cache of balancers by destination name. When a balancer does not exist
in the cache, a new namerd lookup is initiated and its result stream (and value) is cached
so that future connections may resolve quickly. The binder obtains a **balancer** for each
destination name that maintains a list of endpoints and their load (in terms of
connections, throughput, etc).

If the inbound connection has not expired (i.e. due to a timeout), it is dispatched to the
balancer for processing. The balancer maintains a reactor handle and initiates I/O and
balancer state management on the reactor.

```
 ------       ------
| srv0 | ... | srvN |
 ------   |   ------
          |
          | (Envelope, IoStream)
          V
 -------------------      -------------
| binder            |----| interpreter |
 -------------------      -------------
  |
  V
 ----------
| balancer |
 ----------
  |
  V
 ----------
| endpoint |
 ----------
  |
  V
 --------
| duplex |
 --------
```
