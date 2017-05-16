use super::Tls;
use super::super::Connection;
use super::super::lb::{DstConnection, EndpointCtx};
use super::super::socket::{Socket, SecureClientHandshake};
use futures::{Async, Future, Poll};
use std::io;
use tokio_core::net::{TcpStream, TcpStreamNew};
use tokio_core::reactor::Handle;

pub fn new(tcp: TcpStreamNew, tls: Option<Tls>, ctx: EndpointCtx) -> Connecting {
    Connecting(Some(ConnectingState::Connecting(tcp, tls, ctx)))
}

pub struct Connecting(Option<ConnectingState>);
enum ConnectingState {
    Connecting(TcpStreamNew, Option<Tls>, EndpointCtx),
    Handshaking(Box<SecureClientHandshake>, EndpointCtx),
    Ready(Socket, EndpointCtx),
}

impl Future for Connecting {
    type Item = DstConnection;
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        loop {
            let state = self.0
                .take()
                .expect("connect must not be polled after completion");

            match state {
                ConnectingState::Connecting(mut fut, tls, ctx) => {
                    match fut.poll()? {
                        Async::NotReady => {
                            self.0 = Some(ConnectingState::Connecting(fut, tls, ctx));
                            return Ok(Async::NotReady);
                        }
                        Async::Ready(tcp) => {
                            match tls {
                                Some(tls) => {
                                    let conn = Box::new(tls.handshake(tcp));
                                    self.0 = Some(ConnectingState::Handshaking(conn, ctx));
                                }
                                None => {
                                    let sock = Socket::plain(tcp);
                                    self.0 = Some(ConnectingState::Ready(sock, ctx));
                                }
                            }
                        }
                    }
                }

                ConnectingState::Handshaking(mut tls, ctx) => {
                    match tls.poll()? {
                        Async::NotReady => {
                            self.0 = Some(ConnectingState::Handshaking(tls, ctx));
                            return Ok(Async::NotReady);
                        }
                        Async::Ready(sock) => {
                            self.0 = Some(ConnectingState::Ready(sock, ctx));
                        }
                    }
                }

                ConnectingState::Ready(sock, ctx) => {
                    let dst = ctx.dst_name().clone();
                    let conn = Connection::new(ctx, dst, sock);
                    return Ok(Async::Ready(conn));
                }
            }
        }
    }
}
