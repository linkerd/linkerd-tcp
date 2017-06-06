//! TODO `dst_name` should be chosen dynamically.

use super::Path;
use super::connection::{Connection, Socket, ctx, secure, socket};
use super::router::Router;
use futures::{self, Async, Future, Poll, Stream, future};
use rustls;
use std::{io, net, time};
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;
use tacho::{self, Timing};
use tokio_core::net::{TcpListener, Incoming};
use tokio_core::reactor::Handle;
use tokio_timer::Timer;

mod config;
mod sni;
pub use self::config::ServerConfig;

fn unbound(listen_addr: net::SocketAddr,
           dst_name: Path,
           router: Router,
           buf: Rc<RefCell<Vec<u8>>>,
           tls: Option<UnboundTls>,
           metrics: &tacho::Scope)
           -> Unbound {
    let metrics = metrics.clone().prefixed("srv");
    Unbound {
        listen_addr,
        dst_name,
        router,
        buf,
        tls,
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
}
impl Unbound {
    pub fn listen_addr(&self) -> net::SocketAddr {
        self.listen_addr
    }

    pub fn dst_name(&self) -> &Path {
        &self.dst_name
    }

    pub fn bind(self, reactor: &Handle, timer: &Timer) -> io::Result<Bound> {
        debug!("routing on {} to {}", self.listen_addr, self.dst_name);
        let listen = TcpListener::bind(&self.listen_addr, reactor)?;
        let bound_addr = listen.local_addr().unwrap();
        let metrics = self.metrics
            .labeled(SRV_ADDR_KEY, format!("{}", bound_addr));
        Ok(Bound {
               bound_addr,
               reactor: reactor.clone(),
               timer: timer.clone(),
               dst_name: self.dst_name,
               incoming: listen.incoming(),
               tls: self.tls
                   .map(|tls| {
                            BoundTls {
                                config: tls.config,
                                handshake_us: metrics.stat(TLS_HANDSHAKE_US_KEY),
                            }
                        }),
               router: self.router,
               buf: self.buf,
               accepts: metrics.counter(ACCEPTS_KEY),
               closes: metrics.counter(CLOSES_KEY),
               active: metrics.gauge(ACTIVE_KEY),
               conn_metrics: ConnMetrics {
                   rx_bytes: metrics.counter(RX_BYTES_KEY),
                   tx_bytes: metrics.counter(TX_BYTES_KEY),
                   rx_bytes_sum: metrics.stat(CONN_RX_BYTES_KEY),
                   tx_bytes_sum: metrics.stat(CONN_TX_BYTES_KEY),
                   duration_ms: metrics.stat(CONN_DURATION_MS_KEY),
                   latency_us: metrics.stat(CONN_LATENCY_US_KEY),
               },
           })
    }
}

pub struct Bound {
    reactor: Handle,
    timer: Timer,

    bound_addr: net::SocketAddr,
    incoming: Incoming,
    tls: Option<BoundTls>,

    dst_name: Path,
    router: Router,

    buf: Rc<RefCell<Vec<u8>>>,

    accepts: tacho::Counter,
    closes: tacho::Counter,
    active: tacho::Gauge,
    conn_metrics: ConnMetrics,
}

#[derive(Clone)]
struct ConnMetrics {
    rx_bytes: tacho::Counter,
    tx_bytes: tacho::Counter,
    rx_bytes_sum: tacho::Stat,
    tx_bytes_sum: tacho::Stat,
    duration_ms: tacho::Stat,
    latency_us: tacho::Stat,
}

impl Future for Bound {
    type Item = ();
    type Error = io::Error;

    fn poll(&mut self) -> Poll<(), io::Error> {
        // Accept all inbound connections from the listener and spawn their work into the
        // router. This should perhaps yield control back to the reactor periodically.
        loop {
            trace!("{}: polling incoming", self.bound_addr);
            match self.incoming.poll()? {
                Async::NotReady => {
                    return Ok(Async::NotReady);
                }
                Async::Ready(None) => {
                    trace!("{}: listener closed", self.bound_addr);
                    return Ok(Async::Ready(()));
                }
                Async::Ready(Some((tcp, _))) => {
                    trace!("{}: incoming stream from {}",
                           self.bound_addr,
                           tcp.peer_addr().unwrap());

                    let closes = self.closes.clone();
                    let active = self.active.clone();
                    self.accepts.incr(1);
                    active.incr(1);

                    // Finish accepting the connection from the server.
                    //
                    // TODO we should be able to get metadata from a TLS handshake but we can't!
                    let src = {
                        let sock: Box<Future<Item = Socket,
                                             Error = io::Error>> = match self.tls.as_ref() {
                            None => Box::new(future::ok(socket::plain(tcp))),
                            Some(tls) => {
                                let t0 = Timing::start();
                                let hs_us = tls.handshake_us.clone();
                                let sock = secure::server_handshake(tcp, &tls.config)
                                    .map(move |sess| {
                                             hs_us.add(t0.elapsed_us());
                                             socket::secure_server(sess)
                                         });
                                Box::new(sock)
                            }
                        };

                        let dst_name = self.dst_name.clone();
                        let metrics = self.conn_metrics.clone();
                        sock.map(move |sock| {
                            let ctx = SrcCtx {
                                local: sock.local_addr(),
                                peer: sock.peer_addr(),
                                rx_bytes_total: 0,
                                tx_bytes_total: 0,
                                start: Timing::start(),
                                metrics,
                            };
                            Connection::new(dst_name, sock, ctx)
                        })
                    };

                    // Obtain a selector.
                    let balancer = self.router
                        .route(&self.dst_name, &self.reactor, &self.timer);

                    // Once the incoming connection is ready and we have a balancer ready, obtain an
                    // outbound connection and begin streaming. We obtain an outbound connection after
                    // the incoming handshake is complete so that we don't waste outbound connections
                    // on failed inbound connections.
                    let latency_us = self.conn_metrics.latency_us.clone();
                    let connected = futures::lazy(move || {
                        let t0 = Timing::start();
                        src.join(balancer)
                            .and_then(move |(src, balancer)| {
                                          // TODO enforce timeouts here.
                                          latency_us.add(t0.elapsed_us());
                                          balancer.select().map(move |dst| (src, dst))
                                      })
                    });

                    let duplex = {
                        let buf = self.buf.clone();
                        connected.and_then(move |(src, dst)| src.into_duplex(dst, buf))
                    };
                    let done = duplex.then(move |_| {
                                               closes.incr(1);
                                               active.decr(1);
                                               Ok(())
                                           });

                    // Do all of this work in a single, separate task so that we may process
                    // additional connections while this connection is open.
                    //
                    // TODO: implement some sort of backpressure here?
                    self.reactor.spawn(done);
                }
            }
        }
    }
}

#[derive(Clone)]
pub struct UnboundTls {
    config: Arc<rustls::ServerConfig>,
}

#[derive(Clone)]
pub struct BoundTls {
    config: Arc<rustls::ServerConfig>,
    handshake_us: tacho::Stat,
}

static ACCEPTS_KEY: &'static str = "accepts";
static CLOSES_KEY: &'static str = "closes";
static ACTIVE_KEY: &'static str = "connections";
static SRV_ADDR_KEY: &'static str = "srv_addr";
static TLS_HANDSHAKE_US_KEY: &'static str = "tls_handshake_us";
static RX_BYTES_KEY: &'static str = "rx_bytes";
static TX_BYTES_KEY: &'static str = "tx_bytes";
static CONN_RX_BYTES_KEY: &'static str = "connection_rx_bytes";
static CONN_TX_BYTES_KEY: &'static str = "connection_tx_bytes";
static CONN_DURATION_MS_KEY: &'static str = "connection_duration_ms";
static CONN_LATENCY_US_KEY: &'static str = "connect_latency_us";

pub struct SrcCtx {
    local: net::SocketAddr,
    peer: net::SocketAddr,
    rx_bytes_total: usize,
    tx_bytes_total: usize,
    start: time::Instant,
    metrics: ConnMetrics,
}
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
}
impl Drop for SrcCtx {
    fn drop(&mut self) {
        self.metrics.duration_ms.add(self.start.elapsed_ms());
        self.metrics.rx_bytes_sum.add(self.rx_bytes_total as u64);
        self.metrics.tx_bytes_sum.add(self.tx_bytes_total as u64);
    }
}
