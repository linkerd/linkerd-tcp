use std::cell::RefCell;
use std::io;
use std::net::SocketAddr;

use tokio_core::reactor::Handle;
use tokio_core::net::TcpStream;

use futures;
use futures::Future;

// A Basic Pool of connections.
//
// TODO: implement the following
//  [] validity checks (is a connection still valid?)
//  [] error handling (what to do when a connection is no longer valid)
//  [] pool shrinking
//  [] Easy future composition to return a connection
pub struct Pool {
    // We use items as a Stack, always re-using the last one returned.
    items: RefCell<Vec<Box<Future<Item=TcpStream, Error=io::Error>>>>,
    addr: SocketAddr,
    handle: Handle,
}

pub fn new(addr: SocketAddr, handle: Handle) -> Pool {
    Pool {
        items: RefCell::new(Vec::new()),
        addr: addr,
        handle: handle,
    }
}

pub trait ConnectionPool {
    type OnCheckout;
    type OnCheckin;
    fn checkout(&self) -> Self::OnCheckout;
    fn checkin(&self, item: Self::OnCheckin);
}

impl<'a> ConnectionPool for Pool {
    type OnCheckout = Box<Future<Item=TcpStream, Error=io::Error>>;
    type OnCheckin = TcpStream;
    fn checkout(&self) -> Box<Future<Item=TcpStream, Error=io::Error>> {
        if let Some(item) = self.items.borrow_mut().pop() {
            item
        } else {
            // There are no free connections we create one.
            debug!("creating a new Connection to {}", self.addr);
            TcpStream::connect(&self.addr, &self.handle).boxed()
        }
    }

    fn checkin(&self, item: TcpStream) {
        let boxed = futures::future::result(Ok(item)).boxed();
        self.items.borrow_mut().push(boxed);
    }
}
