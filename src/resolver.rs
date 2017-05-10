use super::Path;
use futures::{Stream, Poll};
use std::{io, net};
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
// that.
#[derive(Clone, Debug)]
pub struct Resolver {
    reactor: Remote,
}
impl Resolver {
    pub fn resolve(&mut self, path: Path) -> Resolve {
        Resolve {
            path: path,
            addrs: None,
        }
    }
}

// A stream for a name resolution
#[derive(Clone, Debug)]
pub struct Resolve {
    path: Path,
    addrs: Option<Vec<DstAddr>>,
}

impl Resolve {
    pub fn path(&self) -> &Path {
        &self.path
    }
    pub fn sample(&self) -> &Option<Vec<DstAddr>> {
        &self.addrs
    }
}

impl Stream for Resolve {
    type Item = Vec<DstAddr>;
    type Error = io::Error;
    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        unimplemented!()
    }
}
