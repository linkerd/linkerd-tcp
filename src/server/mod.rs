//! TODO `dst_name` should be chosen dynamically.

use super::Path;
use super::connection::{Connection, Socket, ctx, secure, socket};
use super::router::Router;
use futures::{Async, Future, Poll, Stream, future};
use rustls;
use std::{io, net};
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;
use tacho;
use tokio_core::net::{TcpListener, TcpStream, Incoming};
use tokio_core::reactor::Handle;
use tokio_timer::Timer;

mod config;
mod sni;
pub use self::config::ServerConfig;

const DEFAULT_MAX_CONCURRENCY: usize = 100000;

/// Builds a server that is not yet bound on a port.
fn unbound(listen_addr: net::SocketAddr,
           dst_name: Path,
           router: Router,
           buf: Rc<RefCell<Vec<u8>>>,
           tls: Option<UnboundTls>,
           connect_timeout: Option<Duration>,
           connection_lifetime: Option<Duration>,
           max_concurrency: usize,
           metrics: &tacho::Scope)
           -> Unbound {
    let metrics = metrics.clone().prefixed("srv");
    Unbound {
        listen_addr,
        dst_name,
        router,
        buf,
        tls,
        connect_timeout,
        connection_lifetime,
        max_concurrency,
        metrics,
    }
}

pub struct Unbound {
    listen_addr: net::SocketAddr,
    dst_name: Path,
    router: Router,
    buf: Rc<RefCell<Vec<u8>>>,
    tls: Option<UnboundTls>,
    metrics: tacho::Scope,
    connect_timeout: Option<Duration>,
    connection_lifetime: Option<Duration>,
    max_concurrency: usize,
}
impl Unbound {
    pub fn listen_addr(&self) -> net::SocketAddr {
        self.listen_addr
    }

    pub fn dst_name(&self) -> &Path {
        &self.dst_name
    }

    fn init_src_connection(&self,
                           src_tcp: TcpStream,
                           metrics: &Metrics,
                           tls: &Option<BoundTls>)
                           -> Box<Future<Item = Connection<SrcCtx>, Error = io::Error>> {
        // TODO determine dst_addr dynamically.
        let dst_name = self.dst_name.clone();

        let sock: Box<Future<Item = Socket, Error = io::Error>> = match tls.as_ref() {
            None => future::ok(socket::plain(src_tcp)).boxed(),
            Some(tls) => {
                // TODO we should be able to get metadata from a TLS handshake but we can't!
                let sock = tls.handshake_latency
                    .time(secure::server_handshake(src_tcp, &tls.config))
                    .map(socket::secure_server);
                Box::new(sock)
            }
        };

        let metrics = metrics.per_conn.clone();
        let conn = sock.map(move |sock| {
            let ctx = SrcCtx {
                local: sock.local_addr(),
                peer: sock.peer_addr(),
                rx_bytes_total: 0,
                tx_bytes_total: 0,
                metrics,
            };
            Connection::new(dst_name, sock, ctx)
        });
        Box::new(conn)
    }

    pub fn bind<'a>(self, reactor: &Handle, timer: &Timer) -> io::Result<Bound> {
        debug!("routing on {} to {}", self.listen_addr, self.dst_name);
        let listen = TcpListener::bind(&self.listen_addr, reactor)?;
        let bound_addr = listen.local_addr().unwrap();

        let metrics = self.metrics.labeled("srv_addr", format!("{}", bound_addr));
        let tls = self.tls
            .map(|tls| {
                     BoundTls {
                         config: tls.config,
                         handshake_latency:
                             metrics.clone().prefixed("tls").timer_us("handshake_us"),
                     }
                 });

        let connect_metrics = metrics.clone().prefixed("connect");
        let stream_metrics = metrics.clone().prefixed("stream");
        let per_conn = ConnMetrics {
            rx_bytes: stream_metrics.counter("rx_bytes"),
            tx_bytes: stream_metrics.counter("tx_bytes"),
            rx_bytes_per_conn: stream_metrics.stat("connection_rx_bytes"),
            tx_bytes_per_conn: stream_metrics.stat("connection_tx_bytes"),
            latency: connect_metrics.timer_us("latency_us"),
            duration: stream_metrics.timer_ms("duration_ms"),
        };
        let metrics = Metrics {
            accepts: metrics.counter("accepts"),
            closes: metrics.counter("closes"),
            failures: metrics.counter("failures"),
            active: metrics.gauge("active"),
            waiters: metrics.gauge("waiters"),
            connect_failures: FailureMetrics::new(&connect_metrics, "failure"),
            stream_failures: FailureMetrics::new(&stream_metrics, "failure"),
            per_conn,
        };

        let reactor = reactor.clone();
        let timer = timer.clone();
        let serving = listen
            .incoming()
            .map(move |(src_tcp, _)| {
                metrics.accepts.incr(1);
                let active = metrics.active.clone();
                active.incr(1);
                let waiters = metrics.waiters.clone();
                waiters.incr(1);

                // Finish accepting the connection from the server.
                let src = self.init_src_connection(src_tcp, &metrics, &tls);

                // Obtain a balancing endpoint selector for the given destination.
                let balancer = self.router.route(&self.dst_name, &reactor, &timer);

                // Once the incoming connection is ready and we have a balancer ready, obtain an
                // outbound connection and begin streaming. We obtain an outbound connection after
                // the incoming handshake is complete so that we don't waste outbound connections
                // on failed inbound connections.
                let connect = src.join(balancer)
                    .and_then(move |(src, b)| b.select().map(move |dst| (src, dst)));

                // Enforce a connection timeout, measure successful connection
                // latencies and failure counts.
                let connect = {
                    // Measure the time until the connection is established, if it completes.
                    let c = timeout(metrics.per_conn.latency.time(connect),
                                    self.connect_timeout,
                                    &timer);
                    let fails = metrics.connect_failures.clone();
                    c.then(move |res| {
                               waiters.decr(1);
                               res.map_err(|e| {
                                               fails.record(&e);
                                               e
                                           })
                           })
                };

                // Copy data between the endpoints.
                let stream = {
                    let buf = self.buf.clone();
                    let stream_fails = metrics.stream_failures.clone();
                    let duration = metrics.per_conn.duration.clone();
                    let lifetime = self.connection_lifetime;
                    let timer = timer.clone();
                    connect.and_then(move |(src, dst)| {
                        // Enforce a timeout on total connection lifetime.
                        let duplex = src.into_duplex(dst, buf);
                        duration
                            .time(timeout(duplex, lifetime, &timer))
                            .map_err(move |e| {
                                         stream_fails.record(&e);
                                         e
                                     })
                    })
                };

                let closes = metrics.closes.clone();
                let failures = metrics.failures.clone();
                stream.then(move |ret| {
                                active.decr(1);
                                match ret {
                                    Ok(_) => closes.incr(1),
                                    Err(_) => failures.incr(1),
                                }
                                Ok(())
                            })
            })
            .buffer_unordered(self.max_concurrency);

        Ok(Bound {
               bound_addr,
               serving: Box::new(serving),
           })
    }
}

pub struct Bound {
    bound_addr: net::SocketAddr,
    serving: Box<Stream<Item = (), Error = io::Error> + 'static>,
}
impl Future for Bound {
    type Item = ();
    type Error = io::Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        loop {
            match self.serving.poll() {
                Ok(Async::NotReady) => return Ok(Async::NotReady),
                Ok(Async::Ready(None)) => return Ok(Async::Ready(())),
                Ok(Async::Ready(Some(_))) |
                Err(_) => {}
            }
        }
    }
}

struct Metrics {
    accepts: tacho::Counter,
    closes: tacho::Counter,
    failures: tacho::Counter,
    active: tacho::Gauge,
    waiters: tacho::Gauge,
    per_conn: ConnMetrics,
    connect_failures: FailureMetrics,
    stream_failures: FailureMetrics,
}

#[derive(Clone)]
struct FailureMetrics {
    timeouts: tacho::Counter,
    other: tacho::Counter,
}
impl FailureMetrics {
    fn new(metrics: &tacho::Scope, key: &'static str) -> FailureMetrics {
        FailureMetrics {
            timeouts: metrics.clone().labeled("cause", "timeout").counter(key),
            other: metrics.clone().labeled("cause", "other").counter(key),
        }
    }

    fn record(&self, e: &io::Error) {
        if e.kind() == io::ErrorKind::TimedOut {
            self.timeouts.incr(1);
        } else {
            self.other.incr(1);
        }
    }
}

#[derive(Clone)]
struct ConnMetrics {
    rx_bytes: tacho::Counter,
    tx_bytes: tacho::Counter,
    rx_bytes_per_conn: tacho::Stat,
    tx_bytes_per_conn: tacho::Stat,
    duration: tacho::Timer,
    latency: tacho::Timer,
}

fn timeout<F>(fut: F,
              timeout: Option<Duration>,
              timer: &Timer)
              -> Box<Future<Item = F::Item, Error = io::Error>>
    where F: Future<Error = io::Error> + 'static
{
    match timeout {
        None => Box::new(fut),
        Some(timeout) => Box::new(timer.timeout(fut, timeout)),
    }
}

#[derive(Clone)]
pub struct UnboundTls {
    config: Arc<rustls::ServerConfig>,
}

#[derive(Clone)]
pub struct BoundTls {
    config: Arc<rustls::ServerConfig>,
    handshake_latency: tacho::Timer,
}

pub struct SrcCtx {
    local: net::SocketAddr,
    peer: net::SocketAddr,
    rx_bytes_total: usize,
    tx_bytes_total: usize,
    metrics: ConnMetrics,
}
impl SrcCtx {}
impl ctx::Ctx for SrcCtx {
    fn local_addr(&self) -> net::SocketAddr {
        self.local
    }

    fn peer_addr(&self) -> net::SocketAddr {
        self.peer
    }

    fn read(&mut self, sz: usize) {
        self.rx_bytes_total += sz;
        self.metrics.rx_bytes.incr(sz);
    }

    fn wrote(&mut self, sz: usize) {
        self.tx_bytes_total += sz;
        self.metrics.tx_bytes.incr(sz);
    }

    fn complete(self, _result: io::Result<()>) {
        self.metrics
            .rx_bytes_per_conn
            .add(self.rx_bytes_total as u64);
        self.metrics
            .tx_bytes_per_conn
            .add(self.tx_bytes_total as u64);
    }
}
