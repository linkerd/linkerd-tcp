use super::{DstConnection, DstCtx, Summary};
use super::selector::DstConnectionRequest;
use super::super::Path;
use super::super::connection::{Connection, Socket};
use super::super::connector::{Connector, Connecting};
use futures::{Future, Async};
use futures::unsync::oneshot;
use std::collections::VecDeque;
use std::net;
use tokio_core::reactor::Handle;
use tokio_timer::Timer;

type Completing = oneshot::Receiver<Summary>;

/// Represents a single concrete traffic destination
pub struct Endpoint {
    dst_name: Path,
    peer_addr: net::SocketAddr,

    weight: f32,

    consecutive_failures: usize,

    /// Queues pending connections that have not yet been completed.
    connecting: VecDeque<Connecting>,

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
    completing: VecDeque<Completing>,
}

impl Endpoint {
    pub fn new(dst: Path, addr: net::SocketAddr, weight: f32) -> Endpoint {
        Endpoint {
            dst_name: dst,
            peer_addr: addr,
            weight: weight,
            consecutive_failures: 0,
            connecting: VecDeque::default(),
            connected: VecDeque::default(),
            waiting: VecDeque::default(),
            completing: VecDeque::default(),
        }
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
    pub fn load(&self) -> f32 {
        self.completing.len() as f32 + self.waiting.len() as f32 +
        self.consecutive_failures.pow(2) as f32
    }

    pub fn weight(&self) -> f32 {
        self.weight
    }

    pub fn set_weight(&mut self, w: f32) {
        assert!(0.0 <= w && w <= 1.0);
        self.weight = w;
    }

    pub fn weighted_load(&self) -> f32 {
        self.load() / self.weight
    }

    pub fn init_connecting(&mut self,
                           count: usize,
                           connector: &Connector,
                           reactor: &Handle,
                           timer: &Timer) {
        for _ in 0..count {
            let conn = connector.connect(&self.peer_addr, reactor, timer);
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
                match w.send(conn) {
                    // DstConnectionRequest no longer waiting. save the connection for later.
                    Err(conn) => self.connected.push_front(conn.socket),
                    Ok(_) => self.track_completing(done),
                }
            }
        }
    }

    fn track_connecting(&mut self, mut c: Connecting) {
        match c.poll() {
            Ok(Async::NotReady) => self.connecting.push_back(c),
            Ok(Async::Ready(sock)) => {
                self.connected.push_back(sock);
                self.consecutive_failures = 0;
            }
            Err(_) => {
                self.consecutive_failures += 1;
                error!("{}: connection failed (#{})",
                       self.peer_addr,
                       self.consecutive_failures);
            }
        }
    }

    fn track_completing(&mut self, mut c: Completing) {
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
