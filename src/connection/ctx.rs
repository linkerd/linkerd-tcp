use std::{io, net};

/// A connection context
pub trait Ctx {
    fn local_addr(&self) -> net::SocketAddr;
    fn peer_addr(&self) -> net::SocketAddr;

    fn read(&mut self, sz: usize);
    fn wrote(&mut self, sz: usize);
    fn complete(self, res: io::Result<()>);

    fn join<B: Ctx>(self, other: B) -> Join<Self, B>
        where Self: Sized
    {
        Join(self, other)
    }
}

#[allow(dead_code)]
pub fn null(l: net::SocketAddr, p: net::SocketAddr) -> Null {
    Null(l, p)
}
#[allow(dead_code)]
pub struct Null(net::SocketAddr, net::SocketAddr);
impl Ctx for Null {
    fn local_addr(&self) -> net::SocketAddr {
        self.0
    }

    fn peer_addr(&self) -> net::SocketAddr {
        self.1
    }

    fn read(&mut self, _sz: usize) {}
    fn wrote(&mut self, _sz: usize) {}
    fn complete(self, _res: io::Result<()>) {}
}

pub struct Join<A, B>(A, B);
impl<A: Ctx, B: Ctx> Ctx for Join<A, B> {
    fn local_addr(&self) -> net::SocketAddr {
        self.0.local_addr()
    }

    fn peer_addr(&self) -> net::SocketAddr {
        self.0.peer_addr()
    }

    fn read(&mut self, sz: usize) {
        self.0.wrote(sz);
        self.1.wrote(sz);
    }

    fn wrote(&mut self, sz: usize) {
        self.0.wrote(sz);
        self.1.wrote(sz);
    }

    fn complete(self, result: io::Result<()>) {
        self.0.complete(result.clone());
        self.1.complete(result);
    }
}
