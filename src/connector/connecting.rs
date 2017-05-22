use super::Tls;
use super::super::connection::socket::{self, Socket};
use futures::{Future, Async, Poll};
use std::io;
use tokio_core::net::TcpStreamNew;
use tokio_timer::Sleep;

pub fn new(tcp: TcpStreamNew, tls: Option<Tls>, timeout: Option<Sleep>) -> Connecting {
    let socket: Box<Future<Item = Socket, Error = io::Error>> = match tls {
        None => Box::new(tcp.map(socket::plain)),
        Some(tls) => {
            let sock = tcp.and_then(move |tcp| tls.handshake(tcp))
                .map(socket::secure_client);
            Box::new(sock)
        }
    };
    Connecting { socket, timeout }
}

pub struct Connecting {
    socket: Box<Future<Item = Socket, Error = io::Error>>,
    timeout: Option<Sleep>,
}
impl Future for Connecting {
    type Item = Socket;
    type Error = io::Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match self.socket.poll()? {
            Async::Ready(sock) => Ok(Async::Ready(sock)),
            Async::NotReady => {
                match self.timeout.as_mut() {
                    None => Ok(Async::NotReady),
                    Some(mut timeout) => {
                        match timeout.poll() {
                            Ok(Async::NotReady) => Ok(Async::NotReady),
                            Ok(Async::Ready(_)) => Err(io::ErrorKind::TimedOut.into()),
                            Err(e) => Err(io::Error::new(io::ErrorKind::Other, e)),
                        }
                    }
                }
            }
        }
    }
}
