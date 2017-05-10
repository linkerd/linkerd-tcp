use futures::{Async, AsyncSink, Future};

use lb::{Connector, Duplex, Src, Dst, WithAddr};
use std::{f32, io};
use std::cell::RefCell;
use std::collections::VecDeque;
use std::net::SocketAddr;
use std::rc::Rc;
use std::time::Instant;
use tacho::{self, Timing};

struct Pending {
    connect: Box<Future<Item = Dst, Error = io::Error>>,
    start_t: Instant,
}

struct Established {
    dst: Dst,
    start_t: Instant,
}

struct Active {
    duplex: Duplex,
    start_t: Instant,
}

struct Stats {
    failures: tacho::Counter,
    successes: tacho::Counter,
    connect_latency_us: tacho::Stat,
    connection_ready_ms: tacho::Stat,
    connection_active_ms: tacho::Stat,
    tx_metrics: tacho::Scope,
    rx_metrics: tacho::Scope,
}
impl Stats {
    fn new(metrics: tacho::Scope) -> Stats {
        let tx_metrics = metrics.clone().labeled("direction".into(), "tx".into());
        let rx_metrics = metrics.clone().labeled("direction".into(), "rx".into());
        Stats {
            connect_latency_us: metrics.stat("connect_latency_us".into()),
            connection_ready_ms: metrics.stat("connection_ready_ms".into()),
            connection_active_ms: metrics.stat("connection_active_ms".into()),
            failures: metrics.counter("failure_count".into()),
            successes: metrics.counter("success_count".into()),
            tx_metrics: tx_metrics,
            rx_metrics: rx_metrics,
        }
    }
}

/// A single possibly-available load balancing endpoint.
pub struct Endpoint {
    addr: SocketAddr,
    weight: f32,
    retired: bool,

    /// Pending connection attempts.
    pending: VecDeque<Pending>,

    /// Connections that have been established but are not yet in
    /// active use.
    established: VecDeque<Established>,

    /// Active TCP streams. The stream completed
    active: VecDeque<Active>,

    stats: Stats,
}

impl Endpoint {
    pub fn new(a: SocketAddr, w: f32, metrics: tacho::Scope) -> Endpoint {
        Endpoint {
            addr: a,
            weight: w,
            retired: false,
            pending: VecDeque::new(),
            established: VecDeque::new(),
            active: VecDeque::new(),
            stats: Stats::new(metrics),
        }
    }

    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    pub fn weight(&self) -> f32 {
        self.weight
    }

    pub fn set_weight(&mut self, w: f32) {
        self.weight = w;
    }

    pub fn is_retired(&self) -> bool {
        self.retired
    }

    pub fn retire(&mut self) {
        for _ in 0..self.pending.len() {
            drop(self.pending.pop_front());
        }
        for _ in 0..self.established.len() {
            drop(self.established.pop_front());
        }
        self.retired = true;
    }

    pub fn unretire(&mut self) {
        self.retired = false;
    }

    pub fn conns_pending(&self) -> usize {
        self.pending.len()
    }

    pub fn conns_established(&self) -> usize {
        self.established.len()
    }

    pub fn conns_active(&self) -> usize {
        self.active.len()
    }

    pub fn is_ready(&self) -> bool {
        !self.established.is_empty()
    }

    pub fn is_active(&self) -> bool {
        !self.active.is_empty()
    }

    /// Scores the endpoint.
    ///
    /// Uses the number of active connections, combined with the endpoint's weight, to
    /// produce a load score. A lightly-loaded, heavily-weighted endpoint will receive a
    /// score close to 0.0. An endpoint that cannot currently handle events produces a
    /// score of `f32::INFINITY`.
    ///
    /// TODO: Should this be extracted into a configurable strategy?
    pub fn load(&self) -> f32 {
        if !self.is_ready() {
            // If the endpoint is not ready to serve requests, it should not be
            // considered.
            f32::INFINITY
        } else {
            (1.0 + self.conns_active() as f32) / self.weight
        }
    }

    /// Initiate a new connection
    pub fn init_connection<C: Connector>(&mut self, c: &C) {
        debug!("initiating connection to {}", self.addr);
        self.pending
            .push_back(Pending {
                           start_t: tacho::Timing::start(),
                           connect: c.connect(&self.addr),
                       });
    }

    /// Checks the state of connections for this endpoint.
    ///
    /// When active streams have been completed, they are removed. When pending
    /// connections have been established, they are stored to be dispatched.
    pub fn poll_connections(&mut self) -> io::Result<()> {
        trace!("{}: {} connections established",
               self.addr,
               self.established.len());
        self.poll_active();
        self.poll_pending();
        Ok(())
    }

    fn poll_active(&mut self) {
        let sz = self.active.len();
        trace!("{}: checking {} active streams", self.addr, sz);
        for i in 0..sz {
            let mut active = self.active.pop_front().unwrap();
            trace!("{}: polling active stream {}/{}: {}",
                   self.addr,
                   i + 1,
                   sz,
                   active.duplex.src_addr);
            match active.duplex.poll() {
                Ok(Async::NotReady) => {
                    trace!("{}: still active from {}",
                           self.addr,
                           active.duplex.src_addr);
                    self.active.push_back(active);
                }
                Ok(Async::Ready(())) => {
                    trace!("{}: completed from {}", self.addr, active.duplex.src_addr);
                    self.stats.successes.incr(1);
                    self.stats.connection_active_ms.add(active.start_t.elapsed_ms());
                    drop(active);
                }
                Err(e) => {
                    info!("{}: failed from {}: {}",
                          self.addr,
                          active.duplex.src_addr,
                          e);
                    self.stats.failures.incr(1);
                    self.stats
                        .connection_active_ms
                        .add(active.start_t.elapsed_ms());
                    drop(active);
                }
            }
        }
    }

    fn poll_pending(&mut self) {
        let sz = self.pending.len();
        trace!("{}: polling {} pending streams", self.addr, sz);
        for _ in 0..sz {
            let mut pending = self.pending.pop_front().unwrap();
            match pending.connect.poll() {
                Ok(Async::NotReady) => {
                    self.pending.push_back(pending);
                }
                Ok(Async::Ready(dst)) => {
                    trace!("{}: connection established", self.addr);
                    self.stats
                        .connect_latency_us
                        .add(pending.start_t.elapsed_us());
                    self.established
                        .push_back(Established {
                                       dst: dst,
                                       start_t: tacho::Timing::start(),
                                   });
                }
                Err(e) => {
                    info!("{}: cannot establish connection: {}", self.addr, e);
                }
            }
        }
    }


    /// Attempts to begin transmiting from the the `Src` to this endpoint.
    ///
    /// If no connections have been established, the Upstrea is returned in an
    /// `Async::NotReady` so that the caller may try another endpoint.
    pub fn transmit(&mut self, src: Src, buf: Rc<RefCell<Vec<u8>>>) -> AsyncSink<Src> {
        match self.established.pop_front() {
            None => {
                {
                    let Src(ref src) = src;
                    trace!("no connections to {} from {}", self.addr, src.addr());
                }
                AsyncSink::NotReady(src)
            }
            Some(established) => {
                let Src(src) = src;
                let Dst(dst) = established.dst;
                self.stats
                    .connection_ready_ms
                    .add(established.start_t.elapsed_ms());
                trace!("transmitting to {} from {}", self.addr, src.addr());
                let tx_metrics = self.stats.tx_metrics.clone();
                let rx_metrics = self.stats.rx_metrics.clone();
                self.active
                    .push_front(Active {
                                    duplex: Duplex::new(src, dst, buf, tx_metrics, rx_metrics),
                                    start_t: tacho::Timing::start(),
                                });
                AsyncSink::Ready
            }
        }
    }
}
