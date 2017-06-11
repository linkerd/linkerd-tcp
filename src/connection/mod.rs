use super::Path;
use std::{fmt, net};
use std::cell::RefCell;
use std::rc::Rc;

pub mod ctx;
mod duplex;
mod half_duplex;
pub mod secure;
pub mod socket;

pub use self::ctx::Ctx;
pub use self::duplex::Duplex;
pub use self::socket::Socket;

pub struct ConnectionCtx<C> {
    local_addr: net::SocketAddr,
    peer_addr: net::SocketAddr,
    dst_name: Path,
    ctx: C,
}

impl<C> fmt::Debug for ConnectionCtx<C>
    where C: fmt::Debug
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("ConnectionCtx")
            .field("local_addr", &self.local_addr)
            .field("peer_addr", &self.peer_addr)
            .field("dst_name", &self.dst_name)
            .field("ctx", &self.ctx)
            .finish()
    }
}

impl<C> ConnectionCtx<C>
    where C: Ctx
{
    pub fn new(local_addr: net::SocketAddr,
               peer_addr: net::SocketAddr,
               dst_name: Path,
               ctx: C)
               -> ConnectionCtx<C> {
        ConnectionCtx {
            local_addr,
            peer_addr,
            dst_name,
            ctx,
        }
    }

    pub fn dst_name(&self) -> &Path {
        &self.dst_name
    }

    pub fn ctx(&self) -> &C {
        &self.ctx
    }
}

impl<C> Ctx for ConnectionCtx<C>
    where C: Ctx
{
    fn read(&mut self, sz: usize) {
        self.ctx.read(sz);
    }

    fn wrote(&mut self, sz: usize) {
        self.ctx.wrote(sz);
    }
}
impl<C> Drop for ConnectionCtx<C> {
    fn drop(&mut self) {}
}

/// A src or dst connection.
pub struct Connection<C> {
    pub ctx: ConnectionCtx<C>,
    pub socket: Socket,
}
impl<C: Ctx> Connection<C> {
    pub fn new(dst: Path, socket: Socket, ctx: C) -> Connection<C> {
        let ctx = ConnectionCtx::new(socket.local_addr(), socket.peer_addr(), dst, ctx);
        Connection { socket, ctx }
    }

    pub fn peer_addr(&self) -> net::SocketAddr {
        self.ctx.peer_addr
    }

    pub fn local_addr(&self) -> net::SocketAddr {
        self.ctx.local_addr
    }

    pub fn into_duplex<D: Ctx>(self,
                               other: Connection<D>,
                               buf: Rc<RefCell<Vec<u8>>>)
                               -> Duplex<C, D> {
        duplex::new(self, other, buf)
    }
}
