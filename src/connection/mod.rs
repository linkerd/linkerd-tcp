use std::cell::RefCell;
use std::net;
use std::rc::Rc;

pub mod ctx;
mod duplex;
mod half_duplex;
pub mod secure;
pub mod socket;

pub use self::ctx::Ctx;
pub use self::duplex::Duplex;
pub use self::socket::Socket;

/// A src or dst connection with server or client context.
pub struct Connection<C> {
    /// Record infomation about the connection to be used by a load balance and/or to be
    /// exported as serve/client-scoped metrics.
    pub ctx: C,

    /// Does networked I/O, possibly with TLS.
    pub socket: Socket,
}

impl<C: Ctx> Connection<C> {
    pub fn new(socket: Socket, ctx: C) -> Connection<C> {
        Connection { socket, ctx }
    }

    pub fn peer_addr(&self) -> net::SocketAddr {
        self.socket.peer_addr()
    }

    pub fn local_addr(&self) -> net::SocketAddr {
        self.socket.local_addr()
    }

    /// Transfers data between connections bidirectionally.
    pub fn into_duplex<D: Ctx>(
        self,
        other: Connection<D>,
        buf: Rc<RefCell<Vec<u8>>>,
    ) -> Duplex<C, D> {
        duplex::new(self, other, buf)
    }
}
