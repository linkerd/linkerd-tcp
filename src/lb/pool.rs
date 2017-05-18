use super::{DstConnection, DstAddr};
use super::endpoint::Endpoint;
use super::super::resolver;
use futures::unsync::oneshot;
use ordermap::OrderMap;
use std::cell::RefCell;
use std::collections::VecDeque;
use std::net;

pub type Waiter = oneshot::Sender<DstConnection>;

/// Holds the state of a balancer, including endpoint information and pending connections.
pub struct Pool {
    pub last_result: RefCell<resolver::Result<Vec<DstAddr>>>,

    // Endpoints that may currently be considered for dispatch.
    pub active: RefCell<OrderMap<net::SocketAddr, Endpoint>>,

    // Endpoints that are no longer considered for new requests but that may still be
    // streaming.
    pub retired: RefCell<OrderMap<net::SocketAddr, Endpoint>>,

    pub waiters: RefCell<VecDeque<Waiter>>,
    pub max_waiters: usize,
}

impl Pool {
    pub fn add_waiter(&self, w: Waiter) -> Result<(), Waiter> {
        let mut waiters = self.waiters.borrow_mut();
        if waiters.len() == self.max_waiters {
            return Err(w);
        }
        waiters.push_back(w);
        Ok(())
    }
}
