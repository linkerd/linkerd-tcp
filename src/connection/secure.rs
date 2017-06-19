use futures::{Async, Future, Poll};
use rustls::{Session, ClientConfig, ServerConfig, ClientSession, ServerSession};
use std::fmt;
use std::io::{self, Read, Write};
use std::net::{Shutdown, SocketAddr};
use std::sync::Arc;
use tokio_core::net::TcpStream;
use tokio_io::AsyncWrite;

pub fn client_handshake(tcp: TcpStream, config: &Arc<ClientConfig>, name: &str) -> ClientHandshake {
    let ss = SecureStream::new(tcp, ClientSession::new(config, name));
    ClientHandshake(Some(ss))
}

pub fn server_handshake(tcp: TcpStream, config: &Arc<ServerConfig>) -> ServerHandshake {
    let ss = SecureStream::new(tcp, ServerSession::new(config));
    ServerHandshake(Some(ss))
}

/// Securely transmits data.
pub struct SecureStream<I> {
    peer: SocketAddr,
    local: SocketAddr,
    /// The external encrypted side of the socket.
    tcp: TcpStream,
    /// The internal decrypted side of the socket.
    session: I,
}

impl<S: Session> fmt::Debug for SecureStream<S> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("SecureStream")
            .field("peer", &self.peer)
            .field("local", &self.local)
            .finish()
    }
}

impl<S> SecureStream<S>
where
    S: Session,
{
    fn new(tcp: TcpStream, session: S) -> SecureStream<S> {
        SecureStream {
            peer: tcp.peer_addr().unwrap(),
            local: tcp.local_addr().unwrap(),
            tcp,
            session,
        }
    }

    pub fn peer_addr(&self) -> SocketAddr {
        self.peer
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.local
    }

    pub fn tcp_shutdown(&mut self, how: Shutdown) -> io::Result<()> {
        trace!("tcp_shutdown: {:?}", self);
        self.tcp.shutdown(how)
    }

    fn read_tcp_to_session(&mut self) -> Option<io::Result<usize>> {
        if !self.session.wants_read() {
            trace!("read_tcp_to_session: no read needed: {}", self.peer);
            return None;
        }

        trace!("read_tcp_to_session: read_tls: {}", self.peer);
        match self.session.read_tls(&mut self.tcp) {
            Err(e) => {
                if e.kind() == io::ErrorKind::WouldBlock {
                    trace!("read_tcp_to_session: read_tls: {}: {}", self.peer, e);
                    None
                } else {
                    error!("read_tcp_to_session: read_tls: {}: {}", self.peer, e);
                    Some(Err(e))
                }
            }
            Ok(sz) => {
                trace!("read_tcp_to_session: read_tls: {} {}B", self.peer, sz);
                if sz == 0 {
                    Some(Ok(sz))
                } else {
                    trace!("read_tcp_to_session: process_new_packets: {}", self.peer);
                    match self.session.process_new_packets() {
                        Ok(_) => Some(Ok(sz)),
                        Err(e) => {
                            trace!("read_tcp_to_session: process_new_packets error: {:?}", self);
                            Some(Err(io::Error::new(io::ErrorKind::Other, e)))
                        }
                    }
                }
            }
        }
    }

    fn write_session_to_tcp(&mut self) -> io::Result<usize> {
        trace!("write_session_to_tcp: write_tls: {}", self.peer);
        let sz = self.session.write_tls(&mut self.tcp)?;
        trace!("write_session_to_tcp: write_tls: {}: {}B", self.peer, sz);
        Ok(sz)
    }
}

impl<S> Read for SecureStream<S>
where
    S: Session,
{
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        trace!("read: {}", self.peer);
        let read_ok = match self.read_tcp_to_session() {
            None => false,
            Some(Ok(_)) => true,
            Some(Err(e)) => {
                trace!("read: {}: {:?}", self.peer, e.kind());
                return Err(e);
            }
        };

        let sz = self.session.read(buf)?;
        trace!("read: {}: {}B", self.peer, sz);
        if !read_ok && sz == 0 {
            Err(io::ErrorKind::WouldBlock.into())
        } else {
            Ok(sz)
        }
    }
}

impl<S> Write for SecureStream<S>
where
    S: Session,
{
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        trace!("write: {}", self.peer);
        let sz = self.session.write(buf)?;
        trace!("write: {}: {}B", self.peer, sz);

        {
            let mut write_ok = true;
            while self.session.wants_write() && write_ok {
                write_ok = match self.write_session_to_tcp() {
                    Ok(sz) => sz > 0,
                    Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => false,
                    e @ Err(_) => return e,
                };
            }
        }

        Ok(sz)
    }

    fn flush(&mut self) -> io::Result<()> {
        trace!("flush: {:?}", self);
        self.session.flush()?;
        self.tcp.flush()
    }
}

impl<S> AsyncWrite for SecureStream<S>
where
    S: Session,
{
    fn shutdown(&mut self) -> Poll<(), io::Error> {
        self.session.send_close_notify();
        self.session.write_tls(&mut self.tcp)?;
        self.tcp.flush()?;
        Ok(Async::Ready(()))
    }
}

/// A future that completes when a server's TLS handshake is complete.
#[derive(Debug)]
pub struct ServerHandshake(Option<SecureStream<ServerSession>>);
impl Future for ServerHandshake {
    type Item = SecureStream<ServerSession>;
    type Error = io::Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        trace!("{:?}.poll()", self);
        let mut ss = self.0.take().expect(
            "poll must not be called after completion",
        );

        // Read and write the handshake.
        {
            let mut wrote = true;
            while ss.session.is_handshaking() && wrote {
                if let Some(Err(e)) = ss.read_tcp_to_session() {
                    trace!("server handshake: {}: error: {}", ss.peer, e);
                    return Err(e);
                };
                trace!("server handshake: write_session_to_tcp: {}", ss.peer);
                wrote = ss.session.wants_write() &&
                    match ss.write_session_to_tcp() {
                        Ok(sz) => {
                            trace!(
                                "server handshake: write_session_to_tcp: {}: wrote {}",
                                ss.peer,
                                sz
                            );
                            sz > 0
                        }
                        Err(e) => {
                            trace!("server handshake: write_session_to_tcp: {}: {}", ss.peer, e);
                            if e.kind() != io::ErrorKind::WouldBlock {
                                return Err(e);
                            }
                            false
                        }
                    }
            }
        }

        // If the remote hasn't read everything yet, resume later.
        if ss.session.is_handshaking() {
            trace!("server handshake: {}: not complete", ss.peer);
            self.0 = Some(ss);
            return Ok(Async::NotReady);
        }

        // Finally, acknowledge the handshake is complete.
        if ss.session.wants_write() {
            trace!("server handshake: write_session_to_tcp: {}: final", ss.peer);
            match ss.write_session_to_tcp() {
                Ok(sz) => {
                    trace!(
                        "server handshake: write_session_to_tcp: {}: final: wrote {}B",
                        ss.peer,
                        sz
                    );
                }
                Err(e) => {
                    trace!(
                        "server handshake: write_session_to_tcp: {}: final: {}",
                        ss.peer,
                        e
                    );
                    if e.kind() != io::ErrorKind::WouldBlock {
                        return Err(e);
                    }
                }
            }
        }

        trace!("server handshake: {}: complete", ss.peer);
        Ok(Async::Ready(ss))
    }
}

/// A future that completes when a client's TLS handshake is complete.
#[derive(Debug)]
pub struct ClientHandshake(Option<SecureStream<ClientSession>>);

impl Future for ClientHandshake {
    type Item = SecureStream<ClientSession>;
    type Error = io::Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        trace!("{:?}.poll()", self);
        let mut ss = self.0.take().expect(
            "poll must not be called after completion",
        );

        // Read and write the handshake.
        {
            let mut read_ok = true;
            let mut write_ok = true;
            while ss.session.is_handshaking() && (read_ok || write_ok) {
                trace!("client handshake: read_tcp_to_session: {}", ss.peer);
                read_ok = match ss.read_tcp_to_session() {
                    None => {
                        trace!(
                            "client handshake: read_tcp_to_session: {}: not ready",
                            ss.peer
                        );
                        false
                    }
                    Some(Ok(sz)) => {
                        trace!(
                            "client handshake: read_tcp_to_session: {}: {}B",
                            ss.peer,
                            sz
                        );
                        sz > 0
                    }
                    Some(Err(e)) => {
                        trace!(
                            "client handshake: read_tcp_to_session: {}: error: {}",
                            ss.peer,
                            e
                        );
                        return Err(e);
                    }
                };

                trace!("client handshake: write_session_to_tcp: {}", ss.peer);
                write_ok = ss.session.wants_write() &&
                    match ss.write_session_to_tcp() {
                        Ok(sz) => {
                            trace!(
                                "client handshake: write_session_to_tcp: {}: wrote {}",
                                ss.peer_addr(),
                                sz
                            );
                            sz > 0
                        }
                        Err(e) => {
                            trace!(
                                "client handshake: write_session_to_tcp: {}: {}",
                                ss.peer_addr(),
                                e
                            );
                            if e.kind() != io::ErrorKind::WouldBlock {
                                return Err(e);
                            }
                            false
                        }
                    };
            }
        }

        // If the remote hasn't read everything yet, resume later.
        if ss.session.is_handshaking() {
            trace!("handshake: {}: not complete", ss.peer_addr());
            self.0 = Some(ss);
            return Ok(Async::NotReady);
        }

        trace!("handshake: {}: complete", ss.peer_addr());
        Ok(Async::Ready(ss))
    }
}
