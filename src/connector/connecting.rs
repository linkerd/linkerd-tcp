use super::Tls;
use super::super::connection::socket::{self, Socket};
use futures::{Future, Poll};
use std::io;
use tokio_core::net::TcpStreamNew;

pub fn new(tcp: TcpStreamNew, tls: Option<Tls>) -> Connecting {
    let sock: Box<Future<Item = Socket, Error = io::Error>> = match tls {
        Some(tls) => {
            let sock = tcp.and_then(move |tcp| tls.handshake(tcp))
                .map(socket::secure_client);
            Box::new(sock)
        }
        None => Box::new(tcp.map(socket::plain)),
    };
    Connecting(sock)
}

pub struct Connecting(Box<Future<Item = Socket, Error = io::Error>>);
impl Future for Connecting {
    type Item = Socket;
    type Error = io::Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        self.0.poll()
    }
}
