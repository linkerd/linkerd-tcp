use super::{DstConnection, DstCtx, Summary};
use super::selector::DstConnectionRequest;
use super::super::{Path, Socket};
use super::super::connection::Connection;
use super::super::connector::{Connector, Connecting};
use futures::{Future, Async};
use futures::unsync::oneshot;
use std::collections::VecDeque;
use std::net;
use tokio_core::reactor::Handle;

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

    /// Accepts a request for a connection to be provided.
    ///
    /// The request is satisfied immediately if possible. If there are no available
    /// connections, the request is saved to be satisfied later when there is an available
    /// connection.
    pub fn dispatch_connection(&mut self, select: DstConnectionRequest) {
        match self.connected.pop_front() {
            None => self.waiting.push_back(select),
            Some(sock) => {
                let (conn, done) = self.mk_connection(sock);
                match select.send(conn) {
                    Ok(_) => self.completing.push_back(done),
                    // DstConnectionRequest no longer waiting. save the connection for later.
                    Err(conn) => self.connected.push_front(conn.socket),
                }
            }
        }
    }


    pub fn poll(&mut self) {
        self.poll_connecting();
        self.poll_completing();
        self.poll_waiting();
    }

    pub fn init_connecting(&mut self, count: usize, connector: &Connector, reactor: &Handle) {
        for _ in 0..count {
            let mut conn = connector.connect(&self.peer_addr, reactor);

            // Poll the new connection immediately so that task notification is
            // established.
            match conn.poll() {
                Ok(Async::NotReady) => self.connecting.push_back(conn),
                Ok(Async::Ready(sock)) => {
                    self.connected.push_back(sock);
                    self.consecutive_failures = 0;
                }
                Err(e) => {
                    error!("{}: connection failed: {}", self.peer_addr, e);
                    self.consecutive_failures += 1;
                }
            }
        }
    }

    fn mk_connection(&self, sock: Socket) -> (DstConnection, Completing) {
        let (tx, rx) = oneshot::channel();
        let ctx = DstCtx::new(self.dst_name.clone(), sock.local_addr(), self.peer_addr, tx);
        let conn = Connection::new(self.dst_name.clone(), sock, ctx);
        (conn, rx)
    }

    pub fn is_idle(&self) -> bool {
        self.connecting.is_empty() && self.waiting.is_empty()
    }

    fn poll_connecting(&mut self) {
        for _ in 0..self.connecting.len() {
            let mut fut = self.connecting.pop_front().unwrap();
            match fut.poll() {
                Ok(Async::NotReady) => self.connecting.push_back(fut),
                Ok(Async::Ready(sock)) => self.connected.push_back(sock),
                Err(e) => error!("{}: connection failed: {}", self.peer_addr, e),
            }
        }
    }

    fn poll_completing(&mut self) {
        for _ in 0..self.completing.len() {
            let mut fut = self.completing.pop_front().unwrap();
            match fut.poll() {
                Ok(Async::NotReady) => self.completing.push_back(fut),
                Ok(Async::Ready(conn_summary)) => {
                    debug!("{}: connection complete: {:?}",
                           self.peer_addr,
                           conn_summary)
                }
                Err(_) => error!("{}: lost connection", self.peer_addr),
            }
        }
    }

    fn poll_waiting(&mut self) {
        while let Some(sock) = self.connected.pop_front() {
            let (conn, done) = self.mk_connection(sock);
            if let Err(conn) = self.dispatch_to_next_waiter(conn) {
                self.connected.push_front(conn.socket);
                return;
            } else {
                self.completing.push_back(done);
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
