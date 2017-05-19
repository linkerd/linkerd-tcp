use std::net;

/// A connection context
pub trait Ctx {
    fn local_addr(&self) -> net::SocketAddr;
    fn peer_addr(&self) -> net::SocketAddr;

    fn read(&mut self, sz: usize);
    fn wrote(&mut self, sz: usize);

    fn join<B: Ctx>(self, other: B) -> Join<Self, B>
        where Self: Sized
    {
        Join(self, other)
    }
}

pub struct Null(net::SocketAddr, net::SocketAddr);
pub fn null(l: net::SocketAddr, p: net::SocketAddr) -> Null {
    Null(l, p)
}
impl Ctx for Null {
    fn local_addr(&self) -> net::SocketAddr {
        self.0
    }

    fn peer_addr(&self) -> net::SocketAddr {
        self.1
    }

    fn read(&mut self, _sz: usize) {}
    fn wrote(&mut self, _sz: usize) {}
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
}
