use super::{Path, Socket};
use std::net;

pub mod ctx;
mod duplex;
mod half_duplex;

pub use self::ctx::Ctx;
pub use self::duplex::{Duplex, Summary};

pub struct ConnectionCtx<C> {
    local_addr: net::SocketAddr,
    peer_addr: net::SocketAddr,
    dst_name: Path,
    ctx: C,
}
impl<C: Ctx> ConnectionCtx<C> {
    pub fn new(local: net::SocketAddr,
               peer: net::SocketAddr,
               dst: Path,
               ctx: C)
               -> ConnectionCtx<C> {
        ConnectionCtx {
            local_addr: local,
            peer_addr: peer,
            dst_name: dst,
            ctx: ctx,
        }
    }

    pub fn local_addr(&self) -> net::SocketAddr {
        self.local_addr
    }

    pub fn peer_addr(&self) -> net::SocketAddr {
        self.peer_addr
    }

    pub fn dst_name(&self) -> &Path {
        &self.dst_name
    }

    pub fn ctx(&self) -> &C {
        &self.ctx
    }
}

/// A src or dst connection.
pub struct Connection<C> {
    pub ctx: ConnectionCtx<C>,
    pub socket: Socket,
}
impl<C: Ctx> Connection<C> {
    pub fn new(dst: Path, sock: Socket, ctx: C) -> Connection<C> {
        Connection {
            ctx: ConnectionCtx::new(sock.local_addr(), sock.peer_addr(), dst, ctx),
            socket: sock,
        }
    }

    pub fn peer_addr(&self) -> net::SocketAddr {
        self.ctx.peer_addr
    }

    pub fn local_addr(&self) -> net::SocketAddr {
        self.ctx.local_addr
    }
}
