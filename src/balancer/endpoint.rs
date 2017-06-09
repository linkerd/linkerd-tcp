use super::{DstConnection, DstCtx, Summary};
use super::manager::EndpointMetrics;
use super::selector::DstConnectionRequest;
use super::super::Path;
use super::super::connection::{Connection, Socket};
use super::super::connector::{Connector, Connecting};
use futures::{Future, Async};
use futures::unsync::oneshot;
use std::{cmp, io, net};
use std::collections::VecDeque;
use std::time::Duration;
use tacho::{self, Timing};
use tokio_core::reactor::Handle;
use tokio_timer::{Sleep, Timer};

type Completing = oneshot::Receiver<Summary>;

const BASE_BACKOFF_MS: u64 = 500;
const MAX_BACKOFF_MS: u64 = 60 * 60 * 15; // 15 minutes

/// Represents a single concrete traffic destination
pub struct Endpoint {
    dst_name: Path,
    peer_addr: net::SocketAddr,

    weight: f32,

    reactor: Handle,
    timer: Timer,

    consecutive_failures: u32,
    metrics: EndpointMetrics,
    delay: Option<Sleep>,

    /// Queues pending connections that have not yet been completed.
    connecting: VecDeque<tacho::Timed<Connecting>>,

    /// Queues established connections that have not yet been dispatched.
    connected: VecDeque<Socket>,

    /// Queues dispatch requests for connections.
    waiting: VecDeque<DstConnectionRequest>,

    /// Holds a future that will be completed when streaming is complete.
    ///
    /// ## XXX
    ///
    /// This shold be replaced with a task-aware data structure so that all items
    /// are not polled regularly (so that balancers can scale to 100K+ connections).
    completing: VecDeque<tacho::Timed<Completing>>,
}

impl Endpoint {
    pub fn new(dst_name: Path,
               peer_addr: net::SocketAddr,
               weight: f32,
               metrics: EndpointMetrics,
               reactor: Handle,
               timer: Timer)
               -> Endpoint {
        Endpoint {
            dst_name,
            peer_addr,
            weight,
            reactor,
            timer,
            metrics,
            delay: None,
            consecutive_failures: 0,
            connecting: VecDeque::default(),
            connected: VecDeque::default(),
            waiting: VecDeque::default(),
            completing: VecDeque::default(),
        }
    }

    pub fn peer_addr(&self) -> net::SocketAddr {
        self.peer_addr
    }

    pub fn consecutive_failures(&self) -> u32 {
        self.consecutive_failures
    }

    pub fn connecting(&self) -> usize {
        self.connecting.len()
    }

    pub fn connected(&self) -> usize {
        self.connected.len()
    }

    pub fn completing(&self) -> usize {
        self.completing.len()
    }

    pub fn waiting(&self) -> usize {
        self.waiting.len()
    }

    // TODO should we account for available connections?
    // TODO we should be able to use throughput/bandwidth as well.
    pub fn load(&self) -> usize {
        self.completing.len() + self.waiting.len() + self.consecutive_failures.pow(2) as usize
    }

    pub fn set_weight(&mut self, w: f32) {
        assert!(0.0 <= w && w <= 1.0);
        self.weight = w;
    }

    pub fn weighted_load(&self) -> usize {
        let load = (self.load() + 1) as f32;
        let factor = 100.0 * (1.0 - self.weight);
        (load * factor) as usize
    }

    fn backoff(&self) -> Duration {
        let ms = if self.consecutive_failures == 0 {
            0
        } else {
            BASE_BACKOFF_MS * self.consecutive_failures as u64
        };
        Duration::from_millis(cmp::min(ms, MAX_BACKOFF_MS))
    }

    pub fn init_connecting(&mut self, count: usize, connector: &Connector) {
        if let Some(mut delay) = self.delay.take() {
            debug!("{}: waiting for delay to expire (#{})",
                   self.peer_addr,
                   self.consecutive_failures);
            if delay.poll().expect("failed to sleep") == Async::NotReady {
                self.delay = Some(delay);
                return;
            }
        }

        for _ in 0..count {
            let conn = self.metrics
                .connect_latency
                .time(connector.connect(&self.peer_addr, &self.reactor, &self.timer));
            self.metrics.attempts.incr(1);
            self.track_connecting(conn);
        }
    }

    fn mk_connection(&self, sock: Socket) -> (DstConnection, Completing) {
        let (tx, rx) = oneshot::channel();
        let ctx = DstCtx::new(self.dst_name.clone(), sock.local_addr(), self.peer_addr, tx);
        let conn = Connection::new(self.dst_name.clone(), sock, ctx);
        (conn, rx)
    }

    pub fn is_idle(&self) -> bool {
        self.waiting.is_empty() && self.completing.is_empty()
    }

    /// Accepts a request for a connection to be provided.
    ///
    /// The request is satisfied immediately if possible. If there are no available
    /// connections, the request is saved to be satisfied later when there is an available
    /// connection.
    pub fn track_waiting(&mut self, w: DstConnectionRequest) {
        match self.connected.pop_front() {
            None => self.waiting.push_back(w),
            Some(sock) => {
                let (conn, done) = self.mk_connection(sock);
                let done = self.metrics.connection_duration.time(done);
                match w.send(conn) {
                    // DstConnectionRequest no longer waiting. save the connection for later.
                    Err(conn) => self.connected.push_front(conn.socket),
                    Ok(_) => self.track_completing(done),
                }
            }
        }
    }

    fn track_connecting(&mut self, mut c: tacho::Timed<Connecting>) {
        match c.poll() {
            Ok(Async::NotReady) => self.connecting.push_back(c),
            Ok(Async::Ready(sock)) => {
                self.connected.push_back(sock);
                self.consecutive_failures = 0;
                self.metrics.connects.incr(1);
            }
            Err(e) => {
                error!("{}: connection failed (#{}): {}",
                       self.peer_addr,
                       self.consecutive_failures,
                       e);
                self.consecutive_failures += 1;
                match e.kind() {
                    io::ErrorKind::TimedOut => {
                        self.metrics.timeouts.incr(1);
                    }
                    _ => {
                        self.metrics.failures.incr(1);
                    }
                }

                let backoff = self.backoff();
                debug!("{}: delaying next connection {}ms (#{})",
                       self.peer_addr,
                       backoff.elapsed_ms(),
                       self.consecutive_failures);
                let mut delay = self.timer.sleep(backoff);
                if delay.poll().expect("failed to sleep") == Async::NotReady {
                    self.delay = Some(delay);
                }
            }
        }
    }

    fn track_completing(&mut self, mut c: tacho::Timed<Completing>) {
        match c.poll() {
            Err(_) => error!("{}: connection lost", self.peer_addr),
            Ok(Async::NotReady) => self.completing.push_back(c),
            Ok(Async::Ready(summary)) => {
                debug!("{}: connection finished: {:?}", self.peer_addr, summary);
            }
        }
    }

    pub fn poll_connecting(&mut self) {
        for _ in 0..self.connecting.len() {
            let c = self.connecting.pop_front().unwrap();
            self.track_connecting(c);
        }
    }

    pub fn poll_completing(&mut self) {
        for _ in 0..self.completing.len() {
            let c = self.completing.pop_front().unwrap();
            self.track_completing(c);
        }
    }

    pub fn dispatch_waiting(&mut self) {
        while let Some(sock) = self.connected.pop_front() {
            let (conn, done) = self.mk_connection(sock);
            if let Err(conn) = self.dispatch_to_next_waiter(conn) {
                self.connected.push_front(conn.socket);
                return;
            } else {
                let done = self.metrics.connection_duration.time(done);
                self.track_completing(done);
            }
        }
    }

    fn dispatch_to_next_waiter(&mut self, conn: DstConnection) -> Result<(), DstConnection> {
        if let Some(waiter) = self.waiting.pop_front() {
            return match waiter.send(conn) {
                       Err(conn) => self.dispatch_to_next_waiter(conn),
                       Ok(()) => Ok(()),
                   };
        }
        Err(conn)
    }
}
