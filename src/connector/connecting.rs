use super::Tls;
use super::super::socket::Socket;
use futures::{Future, Poll};
use std::io;
use tokio_core::net::TcpStreamNew;

pub fn new(tcp: TcpStreamNew, tls: Option<Tls>) -> Connecting {
    let sock: Box<Future<Item = Socket, Error = io::Error>> = match tls {
        Some(tls) => Box::new(tcp.and_then(move |tcp| tls.handshake(tcp))),
        None => Box::new(tcp.map(Socket::plain)),
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
