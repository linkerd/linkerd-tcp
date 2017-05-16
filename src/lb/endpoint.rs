use super::connector::ConnectingSocket;
use super::super::{Connection, Path};
use std::cell::RefCell;
use std::collections::VecDeque;
use std::net;
use std::rc::Rc;

pub struct Endpoint {
    pub base_weight: f32,
    pub ctx: EndpointCtx,
    pub connecting: VecDeque<ConnectingSocket>,
    pub connected: VecDeque<Connection<EndpointCtx>>,
}

impl Endpoint {
    pub fn new(dst: Path, addr: net::SocketAddr, base_weight: f32) -> Endpoint {
        Endpoint {
            base_weight: base_weight,
            ctx: EndpointCtx::new(addr, dst),
            connecting: VecDeque::default(),
            connected: VecDeque::default(),
        }
    }

    pub fn clear_connections(&mut self) {
        self.connecting.clear();
        self.connected.clear();
    }
}


/// The state of a load balaner endpoint.
#[derive(Clone, Debug)]
pub struct EndpointCtx(Rc<RefCell<InnerEndpointCtx>>);

#[derive(Debug)]
struct InnerEndpointCtx {
    peer_addr: net::SocketAddr,
    dst_name: Path,
    connect_attempts: usize,
    connect_failures: usize,
    connect_successes: usize,
    disconnects: usize,
    bytes_to_dst: usize,
    bytes_to_src: usize,
}

impl EndpointCtx {
    pub fn new(addr: net::SocketAddr, dst: Path) -> EndpointCtx {
        let inner = InnerEndpointCtx {
            peer_addr: addr,
            dst_name: dst,
            connect_attempts: 0,
            connect_failures: 0,
            connect_successes: 0,
            disconnects: 0,
            bytes_to_dst: 0,
            bytes_to_src: 0,
        };
        EndpointCtx(Rc::new(RefCell::new(inner)))
    }

    pub fn dst_name(&self) -> Path {
        self.0.borrow().dst_name.clone()
    }

    pub fn peer_addr(&self) -> net::SocketAddr {
        let s = self.0.borrow();
        (*s).peer_addr
    }

    pub fn active(&self) -> usize {
        let InnerEndpointCtx {
            connect_successes,
            disconnects,
            ..
        } = *self.0.borrow();

        connect_successes - disconnects
    }

    pub fn connect_init(&self) {
        let mut s = self.0.borrow_mut();
        (*s).connect_attempts += 1;
    }

    pub fn connect_ok(&self) {
        let mut s = self.0.borrow_mut();
        (*s).connect_successes += 1;
    }

    pub fn connect_fail(&self) {
        let mut s = self.0.borrow_mut();
        (*s).connect_failures += 1;
    }

    pub fn disconnect(&self) {
        let mut s = self.0.borrow_mut();
        (*s).disconnects += 1;
    }

    pub fn dst_write(&self, sz: usize) {
        let mut s = self.0.borrow_mut();
        (*s).bytes_to_dst += sz;
    }

    pub fn src_write(&self, sz: usize) {
        let mut s = self.0.borrow_mut();
        (*s).bytes_to_dst += sz;
    }
}
