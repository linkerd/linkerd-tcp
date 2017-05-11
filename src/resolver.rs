use super::{Path, namerd};
use futures::{Future, Sink, Stream, Poll};
use futures::sync::mpsc;
use std::cell::RefCell;
use std::net;
use std::rc::Rc;
use tokio_core::reactor::Remote;

/// A weighted concrete destination address.
#[derive(Clone, Debug)]
pub struct DstAddr {
    pub addr: net::SocketAddr,
    pub weight: f32,
}

impl DstAddr {
    pub fn new(addr: net::SocketAddr, weight: f32) -> DstAddr {
        DstAddr {
            addr: addr,
            weight: weight,
        }
    }
}

// TODO In the future, we likely want to change this to use the split bind & addr APIs so
// balancers can be shared across logical names. In the meantime, it's sufficient to have
// a balancer per logical name.
#[derive(Clone)]
pub struct Resolver {
    reactor: Remote,
    namerd: namerd::Namerd,
}

impl Resolver {
    pub fn resolve(&mut self, path: Path) -> Resolve {
        let addrs = {
            let namerd = self.namerd.clone();
            let path = path.clone();
            let (tx, rx) = mpsc::unbounded();
            let tx = tx.sink_map_err(|_| {});
            self.reactor
                .spawn(move |handle| {
                           let addrs = namerd.resolve(handle, path.as_str());
                           addrs.map_err(|_| {}).forward(tx).map(|_| {})
                       });
            rx
        };
        Resolve(Rc::new(RefCell::new(addrs)))
    }
}

// A stream of name resolutions.
#[derive(Clone)]
pub struct Resolve(Rc<RefCell<mpsc::UnboundedReceiver<namerd::Result<Vec<DstAddr>>>>>);
impl Stream for Resolve {
    type Item = namerd::Result<Vec<DstAddr>>;
    type Error = ();
    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        self.0.borrow_mut().poll()
    }
}
