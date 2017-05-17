use super::Tls;
use super::super::Connection;
use super::super::lb::{DstConnection, EndpointCtx};
use super::super::socket::Socket;
use futures::{Future, Poll};
use std::io;
use tokio_core::net::TcpStreamNew;

pub fn new(tcp: TcpStreamNew, tls: Option<Tls>, ctx: EndpointCtx) -> Connecting {
    let sock: Box<Future<Item = Socket, Error = io::Error>> = match tls {
        Some(tls) => Box::new(tcp.and_then(move |tcp| tls.handshake(tcp))),
        None => Box::new(tcp.map(Socket::plain)),
    };
    let dst = sock.map(|sock| {
                           let dst = ctx.dst_name().clone();
                           Connection::new(ctx, dst, sock)
                       });
    Connecting(Box::new(dst))
}

pub struct Connecting(Box<Future<Item = DstConnection, Error = io::Error>>);
impl Future for Connecting {
    type Item = DstConnection;
    type Error = io::Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        self.0.poll()
    }
}
