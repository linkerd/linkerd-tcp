use super::{Connection, DstAddr, namerd};
use futures::{Future, Sink, Poll, Async, StartSend, AsyncSink};
use ordermap::OrderMap;
use std::{cmp, io, net};
use std::cell::RefCell;
use std::collections::VecDeque;
use std::rc::Rc;
use twox_hash::RandomXxHashBuilder;

#[derive(Clone, Hash, PartialEq, Eq)]
pub struct Config {
    pub pending_conns: usize,
}
impl Default for Config {
    fn default() -> Config {
        Config { pending_conns: 1 }
    }
}

impl Config {
    pub fn build(self, addrs: namerd::Result<Vec<DstAddr>>) -> Balancer {
        let active = if let Ok(addrs) = addrs {
            let mut active = OrderMap::with_capacity_and_hasher(addrs.len(),
                                                                RandomXxHashBuilder::default());
            for &DstAddr { addr, weight } in &addrs {
                let mut ep = Endpoint::default();
                ep.base_weight = weight;
                active.insert(addr, ep);
            }
            active
        } else {
            OrderMap::default()
        };

        Balancer {
            config: Rc::new(self),
            active: Rc::new(RefCell::new(active)),
            retired: Rc::new(RefCell::new(OrderMap::default())),
        }
    }
}

#[derive(Clone)]
pub struct Balancer {
    config: Rc<Config>,
    active: Rc<RefCell<OrderMap<net::SocketAddr, Endpoint, RandomXxHashBuilder>>>,
    retired: Rc<RefCell<OrderMap<net::SocketAddr, Endpoint, RandomXxHashBuilder>>>,
}

impl Balancer {
    pub fn connect(&mut self) -> Connect {
        unimplemented!()
    }
}

/// Balancers accept a stream of service discovery updates,
impl Sink for Balancer {
    type SinkItem = namerd::Result<Vec<DstAddr>>;
    type SinkError = ();

    /// Update the load balancer from service discovery.
    fn start_send(&mut self,
                  update: namerd::Result<Vec<DstAddr>>)
                  -> StartSend<namerd::Result<Vec<DstAddr>>, Self::SinkError> {
        if let Ok(dsts) = update {
            let mut dsts = {
                let mut map = OrderMap::with_capacity_and_hasher(dsts.len(),
                                                                 RandomXxHashBuilder::default());
                for &DstAddr { addr, weight } in &dsts {
                    map.insert(addr, weight);
                }
                map
            };
            let mut active = self.active.borrow_mut();
            let mut retired = self.retired.borrow_mut();
            let mut temp = VecDeque::with_capacity(cmp::max(active.len(), retired.len()));

            // Check retired endpoints.
            //
            // Endpoints are either salvaged backed into the active pool, maintained as
            // retired if still active, or dropped if inactive.
            {
                for (addr, ep) in (*retired).drain(..) {
                    if dsts.contains_key(&addr) {
                        (*active).insert(addr, ep);
                    } else if ep.active() > 0 {
                        temp.push_back((addr, ep));
                    } else {
                        drop(ep);
                    }
                }
                for _ in 0..temp.len() {
                    let (addr, ep) = temp.pop_front().unwrap();
                    (*retired).insert(addr, ep);
                }
            }

            // Determine which active endpoints are to be retired.
            {
                for (addr, ep) in (*active).drain(..) {
                    if dsts.contains_key(&addr) {
                        temp.push_back((addr, ep));
                    } else if ep.active() > 0 {
                        (*retired).insert(addr, ep);
                    } else {
                        drop(ep);
                    }
                }
                for _ in 0..temp.len() {
                    let (addr, ep) = temp.pop_front().unwrap();
                    (*active).insert(addr, ep);
                }
            }

            for (addr, weight) in dsts.drain(..) {
                if let Some(mut ep) = (*active).get_mut(&addr) {
                    ep.base_weight = weight;
                    continue;
                }

                let mut ep = Endpoint::default();
                ep.base_weight = weight;
                (*active).insert(addr, ep);
            }
        }

        trace!("start_send: NotReady");
        Ok(AsyncSink::Ready)
    }

    /// Never completes.
    fn poll_complete(&mut self) -> Poll<(), Self::SinkError> {
        trace!("poll_complete: NotReady");
        // TODO should we check the state of the balancer here as well?
        Ok(Async::NotReady)
    }
}

#[derive(Clone)]
struct Endpoint {
    base_weight: f32,
    state: Rc<RefCell<EndpointState>>,
}
impl Endpoint {
    fn active(&self) -> usize {
        self.state.borrow().active
    }
}
impl Default for Endpoint {
    fn default() -> Endpoint {
        Endpoint {
            base_weight: 1.0,
            state: Rc::new(RefCell::new(EndpointState::default())),
        }
    }
}

#[derive(Clone, Default)]
struct EndpointState {
    connecting: usize,
    active: usize,
}

pub struct Connect(Rc<RefCell<EndpointState>>);
impl Future for Connect {
    type Item = Connection;
    type Error = io::Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        unimplemented!()
    }
}
