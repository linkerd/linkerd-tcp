use futures::{Async, Future, Poll};
use rustls::{Session, ClientConfig, ServerConfig, ClientSession, ServerSession};
use std::fmt;
use std::io::{self, Read, Write};
use std::net::{Shutdown, SocketAddr};
use std::sync::Arc;
use tokio_core::net::TcpStream;
use tokio_io::AsyncWrite;

/// Hides the implementation details of socket I/O.
///
/// Plaintext and encrypted (client and server) streams have different type signatures.
/// Exposing these types to the rest of the application is painful, so `Socket` provides
/// an opaque container for the various types of sockets supported by this proxy.
#[derive(Debug)]
pub struct Socket(Inner);

// Since the rustls types are much larger than the plain types, they are boxed. because
// clippy says so.
enum Inner {
    Plain(TcpStream),
    SecureClient(Box<SecureSocket<ClientSession>>),
    SecureServer(Box<SecureSocket<ServerSession>>),
}

impl fmt::Debug for Inner {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Inner::Plain(ref s) => {
                f.debug_struct("Plain")
                    .field("peer", &s.peer_addr().unwrap())
                    .field("local", &s.local_addr().unwrap())
                    .finish()
            }
            Inner::SecureClient(ref s) => {
                f.debug_struct("SecureClient")
                    .field("peer", &s.peer_addr())
                    .field("local", &s.local_addr())
                    .finish()
            }
            Inner::SecureServer(ref s) => {
                f.debug_struct("SecureServer")
                    .field("peer", &s.peer_addr())
                    .field("local", &s.local_addr())
                    .finish()
            }
        }
    }
}

impl Socket {
    pub fn plain(stream: TcpStream) -> Socket {
        Socket(Inner::Plain(stream))
    }

    pub fn secure_client_handshake(tcp: TcpStream,
                                   tls: &Arc<ClientConfig>,
                                   name: &str)
                                   -> SecureClientHandshake {
        trace!("initializing client handshake");
        let s = SecureSocket::new(tcp, ClientSession::new(tls, name));
        SecureClientHandshake(Some(s))
    }

    pub fn secure_server_handshake(tcp: TcpStream,
                                   tls: &Arc<ServerConfig>)
                                   -> SecureServerHandshake {
        trace!("initializing server handshake");
        let s = SecureSocket::new(tcp, ServerSession::new(tls));
        SecureServerHandshake(Some(s))
    }

    pub fn tcp_shutdown(&mut self, how: Shutdown) -> io::Result<()> {
        trace!("{:?}.tcp_shutdown({:?})", self, how);
        match self.0 {
            Inner::Plain(ref mut stream) => TcpStream::shutdown(stream, how),
            Inner::SecureClient(ref mut stream) => stream.tcp_shutdown(how),
            Inner::SecureServer(ref mut stream) => stream.tcp_shutdown(how),
        }
    }

    pub fn local_addr(&self) -> SocketAddr {
        match self.0 {
            Inner::Plain(ref stream) => stream.local_addr().unwrap(),
            Inner::SecureClient(ref stream) => stream.local_addr(),
            Inner::SecureServer(ref stream) => stream.local_addr(),
        }
    }

    pub fn peer_addr(&self) -> SocketAddr {
        match self.0 {
            Inner::Plain(ref stream) => stream.peer_addr().unwrap(),
            Inner::SecureClient(ref stream) => stream.peer_addr(),
            Inner::SecureServer(ref stream) => stream.peer_addr(),
        }
    }
}

/// Reads the socket without blocking.
impl Read for Socket {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        trace!("{:?}.read({})", self, buf.len());
        match self.0 {
            Inner::Plain(ref mut stream) => stream.read(buf),
            Inner::SecureClient(ref mut stream) => stream.read(buf),
            Inner::SecureServer(ref mut stream) => stream.read(buf),
        }
    }
}

/// Writes to the socket without blocking.
impl Write for Socket {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        trace!("{:?}.write({})", self, buf.len());
        match self.0 {
            Inner::Plain(ref mut stream) => stream.write(buf),
            Inner::SecureClient(ref mut stream) => stream.write(buf),
            Inner::SecureServer(ref mut stream) => stream.write(buf),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        trace!("{:?}.flush()", self);
        match self.0 {
            Inner::Plain(ref mut stream) => stream.flush(),
            Inner::SecureClient(ref mut stream) => stream.flush(),
            Inner::SecureServer(ref mut stream) => stream.flush(),
        }
    }
}

/// Closes the write-side of a stream.
impl AsyncWrite for Socket {
    fn shutdown(&mut self) -> Poll<(), io::Error> {
        trace!("{:?}.shutdown()", self);
        match self.0 {
            Inner::Plain(ref mut stream) => AsyncWrite::shutdown(stream),
            Inner::SecureClient(ref mut stream) => stream.shutdown(),
            Inner::SecureServer(ref mut stream) => stream.shutdown(),
        }
    }
}

/// Securely transmits data.
struct SecureSocket<I> {
    peer: SocketAddr,
    local: SocketAddr,
    /// The external encrypted side of the socket.
    tcp: TcpStream,
    /// The internal decrypted side of the socket.
    session: I,
}

impl<S: Session> fmt::Debug for SecureSocket<S> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("SecureSocket")
            .field("peer", &self.peer)
            .field("local", &self.local)
            .finish()
    }
}

impl<S> SecureSocket<S>
    where S: Session
{
    pub fn new(t: TcpStream, s: S) -> SecureSocket<S> {
        SecureSocket {
            peer: t.peer_addr().unwrap(),
            local: t.local_addr().unwrap(),
            tcp: t,
            session: s,
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

impl<S> Read for SecureSocket<S>
    where S: Session
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

impl<S> Write for SecureSocket<S>
    where S: Session
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

impl<S> AsyncWrite for SecureSocket<S>
    where S: Session
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
pub struct SecureServerHandshake(Option<SecureSocket<ServerSession>>);
impl Future for SecureServerHandshake {
    type Item = Socket;
    type Error = io::Error;
    fn poll(&mut self) -> Poll<Socket, io::Error> {
        trace!("{:?}.poll()", self);
        let mut ss = self.0
            .take()
            .expect("poll must not be called after completion");

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
                    trace!("server handshake: write_session_to_tcp: {}: wrote {}",
                           ss.peer,
                           sz);
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
                    trace!("server handshake: write_session_to_tcp: {}: final: wrote {}B",
                           ss.peer,
                           sz);
                }
                Err(e) => {
                    trace!("server handshake: write_session_to_tcp: {}: final: {}",
                           ss.peer,
                           e);
                    if e.kind() != io::ErrorKind::WouldBlock {
                        return Err(e);
                    }
                }
            }
        }

        trace!("server handshake: {}: complete", ss.peer);
        Ok(Socket(Inner::SecureServer(Box::new(ss))).into())
    }
}

/// A future that completes when a client's TLS handshake is complete.
#[derive(Debug)]
pub struct SecureClientHandshake(Option<SecureSocket<ClientSession>>);
impl Future for SecureClientHandshake {
    type Item = Socket;
    type Error = io::Error;
    fn poll(&mut self) -> Poll<Socket, io::Error> {
        trace!("{:?}.poll()", self);
        let mut ss = self.0
            .take()
            .expect("poll must not be called after completion");

        // Read and write the handshake.
        {
            let mut read_ok = true;
            let mut write_ok = true;
            while ss.session.is_handshaking() && (read_ok || write_ok) {
                trace!("client handshake: read_tcp_to_session: {}", ss.peer);
                read_ok = match ss.read_tcp_to_session() {
                    None => {
                        trace!("client handshake: read_tcp_to_session: {}: not ready",
                               ss.peer);
                        false
                    }
                    Some(Ok(sz)) => {
                        trace!("client handshake: read_tcp_to_session: {}: {}B",
                               ss.peer,
                               sz);
                        sz > 0
                    }
                    Some(Err(e)) => {
                        trace!("client handshake: read_tcp_to_session: {}: error: {}",
                               ss.peer,
                               e);
                        return Err(e);
                    }
                };

                trace!("client handshake: write_session_to_tcp: {}", ss.peer);
                write_ok = ss.session.wants_write() &&
                           match ss.write_session_to_tcp() {
                               Ok(sz) => {
                    trace!("client handshake: write_session_to_tcp: {}: wrote {}",
                           ss.peer_addr(),
                           sz);
                    sz > 0
                }
                               Err(e) => {
                    trace!("client handshake: write_session_to_tcp: {}: {}",
                           ss.peer_addr(),
                           e);
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
        Ok(Socket(Inner::SecureClient(Box::new(ss))).into())
    }
}
