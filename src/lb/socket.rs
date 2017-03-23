use futures::{Async, Future, Poll};

use lb::WithAddr;
use rustls::{Session, ClientConfig, ServerConfig, ClientSession, ServerSession};
use std::fmt;
use std::io::{self, Read, Write};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio_core::net::TcpStream;
use tokio_io::{AsyncRead, AsyncWrite};

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
    Plain(SocketAddr, TcpStream),
    SecureClient(SocketAddr, Box<SecureSocket<TcpStream, ClientSession>>),
    SecureServer(SocketAddr, Box<SecureSocket<TcpStream, ServerSession>>),
}

impl fmt::Debug for Inner {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Inner::Plain(ref a, _) => f.debug_tuple("Plain").field(a).finish(),
            Inner::SecureClient(ref a, _) => f.debug_tuple("SecureClient").field(a).finish(),
            Inner::SecureServer(ref a, _) => f.debug_tuple("SecureServer").field(a).finish(),
        }
    }
}

impl Socket {
    pub fn plain(addr: SocketAddr, tcp: TcpStream) -> Socket {
        Socket(Inner::Plain(addr, tcp))
    }

    pub fn secure_client_handshake(addr: SocketAddr,
                                   tcp: TcpStream,
                                   tls: &Arc<ClientConfig>,
                                   name: &str)
                                   -> SecureClientHandshake {
        trace!("initializing client handshake");
        let s = SecureSocket::new(addr, tcp, ClientSession::new(tls, name));
        SecureClientHandshake(Some(s))
    }

    pub fn secure_server_handshake(addr: SocketAddr,
                                   tcp: TcpStream,
                                   tls: &Arc<ServerConfig>)
                                   -> SecureServerHandshake {
        trace!("initializing server handshake");
        let s = SecureSocket::new(addr, tcp, ServerSession::new(tls));
        SecureServerHandshake(Some(s))
    }
}

impl WithAddr for Socket {
    fn addr(&self) -> SocketAddr {
        match self.0 {
            Inner::Plain(ref a, _) |
            Inner::SecureClient(ref a, _) |
            Inner::SecureServer(ref a, _) => *a,
        }
    }
}

/// Reads the socket without blocking.
impl Read for Socket {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        trace!("{:?}.read({})", self, buf.len());
        match self.0 {
            Inner::Plain(_, ref mut t) => t.read(buf),
            Inner::SecureClient(_, ref mut c) => c.read(buf),
            Inner::SecureServer(_, ref mut s) => s.read(buf),
        }
    }
}

/// Writes to the socket without blocking.
impl Write for Socket {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        trace!("{:?}.write({})", self, buf.len());
        match self.0 {
            Inner::Plain(_, ref mut t) => t.write(buf),
            Inner::SecureClient(_, ref mut c) => c.write(buf),
            Inner::SecureServer(_, ref mut s) => s.write(buf),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        trace!("{:?}.flush()", self);
        match self.0 {
            Inner::Plain(_, ref mut t) => t.flush(),
            Inner::SecureClient(_, ref mut c) => c.flush(),
            Inner::SecureServer(_, ref mut s) => s.flush(),
        }
    }
}

/// Closes the write-side of a stream.
impl AsyncWrite for Socket {
    fn shutdown(&mut self) -> Poll<(), io::Error> {
        trace!("{:?}.shutdown()", self);
        match self.0 {
            Inner::Plain(_, ref mut t) => t.shutdown(),
            Inner::SecureClient(_, ref mut c) => c.shutdown(),
            Inner::SecureServer(_, ref mut s) => s.shutdown(),
        }
    }
}

/// Securely transmits data.
struct SecureSocket<E, I> {
    addr: SocketAddr,
    /// The external encrypted side of the socket.
    ext: E,
    /// The internal decrypted side of the socket.
    int: I,
}

impl<E, I> fmt::Debug for SecureSocket<E, I> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_tuple("SecureSocket").field(&self.addr).finish()
    }
}

impl<E, I> SecureSocket<E, I>
    where E: AsyncRead + AsyncWrite,
          I: Session
{
    pub fn new(a: SocketAddr, e: E, i: I) -> SecureSocket<E, I> {
        SecureSocket {
            addr: a,
            ext: e,
            int: i,
        }
    }

    fn read_ext_to_int(&mut self) -> Option<io::Result<usize>> {
        if !self.int.wants_read() {
            trace!("read_ext_to_int: no read needed: {}", self.addr);
            return None;
        }

        trace!("read_ext_to_int: read_tls: {}", self.addr);
        match self.int.read_tls(&mut self.ext) {
            Err(e) => {
                if e.kind() == io::ErrorKind::WouldBlock {
                    trace!("read_ext_to_int: read_tls: {}: {}", self.addr, e);
                    None
                } else {
                    error!("read_ext_to_int: read_tls: {}: {}", self.addr, e);
                    Some(Err(e))
                }
            }
            Ok(sz) => {
                trace!("read_ext_to_int: read_tls: {} {}B", self.addr, sz);
                if sz == 0 {
                    Some(Ok(sz))
                } else {
                    trace!("read_ext_to_int: process_new_packets: {}", self.addr);
                    match self.int.process_new_packets() {
                        Ok(_) => Some(Ok(sz)),
                        Err(e) => {
                            trace!("read_ext_to_int: process_new_packets error: {:?}", self);
                            Some(Err(io::Error::new(io::ErrorKind::Other, e)))
                        }
                    }
                }
            }
        }
    }

    fn write_int_to_ext(&mut self) -> io::Result<usize> {
        trace!("write_int_to_ext: write_tls: {}", self.addr);
        let sz = self.int.write_tls(&mut self.ext)?;
        trace!("write_int_to_ext: write_tls: {}: {}B", self.addr, sz);
        Ok(sz)
    }
}

impl<E, D> WithAddr for SecureSocket<E, D> {
    fn addr(&self) -> SocketAddr {
        self.addr
    }
}

impl<E, D> Read for SecureSocket<E, D>
    where E: AsyncRead + AsyncWrite,
          D: Session
{
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        trace!("read: {}", self.addr);
        let read_ok = match self.read_ext_to_int() {
            None => false,
            Some(Ok(_)) => true,
            Some(Err(e)) => {
                trace!("read: {}: {:?}", self.addr, e.kind());
                return Err(e);
            }
        };

        let sz = self.int.read(buf)?;
        trace!("read: {}: {}B", self.addr, sz);
        if !read_ok && sz == 0 {
            Err(io::ErrorKind::WouldBlock.into())
        } else {
            Ok(sz)
        }
    }
}

impl<E, D> Write for SecureSocket<E, D>
    where E: AsyncRead + AsyncWrite,
          D: Session
{
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        trace!("write: {}", self.addr);
        let sz = self.int.write(buf)?;
        trace!("write: {}: {}B", self.addr, sz);

        {
            let mut write_ok = true;
            while self.int.wants_write() && write_ok {
                write_ok = match self.write_int_to_ext() {
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
        self.int.flush()?;
        self.ext.flush()
    }
}

impl<E, D> AsyncWrite for SecureSocket<E, D>
    where E: AsyncRead + AsyncWrite,
          D: Session
{
    fn shutdown(&mut self) -> Poll<(), io::Error> {
        trace!("shutdown: {:?}", self);
        self.int.send_close_notify();
        self.int.write_tls(&mut self.ext)?;
        self.ext.shutdown()?;
        Ok(Async::Ready(()))
    }
}

/// A future that completes when a server's TLS handshake is complete.
#[derive(Debug)]
pub struct SecureServerHandshake(Option<SecureSocket<TcpStream, ServerSession>>);
impl Future for SecureServerHandshake {
    type Item = Socket;
    type Error = io::Error;
    fn poll(&mut self) -> Poll<Socket, io::Error> {
        trace!("{:?}.poll()", self);
        let mut ss = self.0.take().expect("poll must not be called after completion");

        // Read and write the handshake.
        {
            let mut wrote = true;
            while ss.int.is_handshaking() && wrote {
                if let Some(Err(e)) = ss.read_ext_to_int() {
                    trace!("server handshake: {}: error: {}", ss.addr, e);
                    return Err(e);
                };
                trace!("server handshake: write_int_to_ext: {}", ss.addr);
                wrote = ss.int.wants_write() &&
                        match ss.write_int_to_ext() {
                    Ok(sz) => {
                        trace!("server handshake: write_int_to_ext: {}: wrote {}",
                               ss.addr,
                               sz);
                        sz > 0
                    }
                    Err(e) => {
                        trace!("server handshake: write_int_to_ext: {}: {}", ss.addr, e);
                        if e.kind() != io::ErrorKind::WouldBlock {
                            return Err(e);
                        }
                        false
                    }
                }
            }
        }

        // If the remote hasn't read everything yet, resume later.
        if ss.int.is_handshaking() {
            trace!("server handshake: {}: not complete", ss.addr);
            self.0 = Some(ss);
            return Ok(Async::NotReady);
        }

        // Finally, acknowledge the handshake is complete.
        if ss.int.wants_write() {
            trace!("server handshake: write_int_to_ext: {}: final", ss.addr);
            match ss.write_int_to_ext() {
                Ok(sz) => {
                    trace!("server handshake: write_int_to_ext: {}: final: wrote {}B",
                           ss.addr,
                           sz);
                }
                Err(e) => {
                    trace!("server handshake: write_int_to_ext: {}: final: {}",
                           ss.addr,
                           e);
                    if e.kind() != io::ErrorKind::WouldBlock {
                        return Err(e);
                    }
                }
            }
        }

        trace!("server handshake: {}: complete", ss.addr);
        Ok(Socket(Inner::SecureServer(ss.addr, Box::new(ss))).into())
    }
}

/// A future that completes when a client's TLS handshake is complete.
#[derive(Debug)]
pub struct SecureClientHandshake(Option<SecureSocket<TcpStream, ClientSession>>);
impl Future for SecureClientHandshake {
    type Item = Socket;
    type Error = io::Error;
    fn poll(&mut self) -> Poll<Socket, io::Error> {
        trace!("{:?}.poll()", self);
        let mut ss = self.0.take().expect("poll must not be called after completion");

        // Read and write the handshake.
        {
            let mut read_ok = true;
            let mut write_ok = true;
            while ss.int.is_handshaking() && (read_ok || write_ok) {
                trace!("client handshake: read_ext_to_int: {}", ss.addr);
                read_ok = match ss.read_ext_to_int() {
                    None => {
                        trace!("client handshake: read_ext_to_int: {}: not ready", ss.addr);
                        false
                    }
                    Some(Ok(sz)) => {
                        trace!("client handshake: read_ext_to_int: {}: {}B", ss.addr, sz);
                        sz > 0
                    }
                    Some(Err(e)) => {
                        trace!("client handshake: read_ext_to_int: {}: error: {}",
                               ss.addr,
                               e);
                        return Err(e);
                    }
                };

                trace!("client handshake: write_int_to_ext: {}", ss.addr);
                write_ok = ss.int.wants_write() &&
                           match ss.write_int_to_ext() {
                    Ok(sz) => {
                        trace!("client handshake: write_int_to_ext: {}: wrote {}",
                               ss.addr,
                               sz);
                        sz > 0
                    }
                    Err(e) => {
                        trace!("client handshake: write_int_to_ext: {}: {}", ss.addr, e);
                        if e.kind() != io::ErrorKind::WouldBlock {
                            return Err(e);
                        }
                        false
                    }
                };
            }
        }

        // If the remote hasn't read everything yet, resume later.
        if ss.int.is_handshaking() {
            trace!("handshake: {}: not complete", ss.addr);
            self.0 = Some(ss);
            return Ok(Async::NotReady);
        }

        trace!("handshake: {}: complete", ss.addr);
        Ok(Socket(Inner::SecureClient(ss.addr, Box::new(ss))).into())
    }
}
