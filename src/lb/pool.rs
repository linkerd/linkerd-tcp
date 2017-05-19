use super::{DstConnection, DstAddr};
//use super::endpoint::Endpoint;
use super::super::Path;
use super::super::connector::Connecting;
use super::super::resolver;
use futures::unsync::{oneshot, mpsc};
use ordermap::OrderMap;
use std::cell::RefCell;
use std::collections::VecDeque;
use std::net;
use std::rc::Rc;

pub type Waiter = oneshot::Sender<DstConnection>;

/// Holds the state of a balancer, including endpoint information and pending connections.
pub struct Pool {
    // Endpoints that may currently be considered for dispatch.
    active: OrderMap<net::SocketAddr, Endpoint>,
    retired: OrderMap<net::SocketAddr, Endpoint>,

    pub ctx: Rc<RefCell<PoolCtx>>,
}

struct PoolCtx {
    pub waiters_permitted: usize,
    pub pending_connections: usize,
    pub established_connections: usize,
    pub active_streams: usize,
    pub bytes_to_dst: usize,
    pub bytes_from_dst: usize,
}

struct Endpoint {
    weight: f32,
    load: f32,
    should_evict: bool,
    connecting: VecDeque<Connecting>,
    connected: VecDeque<DstConnection>,
    waiters: mpsc::UnboundedReceiver<Waiter>,
    completions: mpsc::UnboundedReceiver<ConnectionSummary>,
}

impl Endpoint {
    pub fn new(dst: Path, addr: net::SocketAddr, weight: f32) -> Endpoint {
        Endpoint {
            weight: weight,
            load: ::std::f32::MAX,
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

pub struct ConnectionSummary {
    local_addr: net::SocketAddr,
    peer_addr: net::SocketAddr,
    read_bytes: usize,
    summary_bytes: usize,
}
