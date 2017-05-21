use super::secure::SecureStream;
use futures::Poll;
use rustls::{ClientSession, ServerSession};
use std::fmt;
use std::io::{self, Read, Write};
use std::net::{Shutdown, SocketAddr};
use tokio_core::net::TcpStream;
use tokio_io::AsyncWrite;

pub fn plain(tcp: TcpStream) -> Socket {
    Socket(Kind::Plain(tcp))
}

pub fn secure_client(tls: SecureStream<ClientSession>) -> Socket {
    Socket(Kind::SecureClient(Box::new(tls)))
}

pub fn secure_server(tls: SecureStream<ServerSession>) -> Socket {
    Socket(Kind::SecureServer(Box::new(tls)))
}

/// Hides the implementation details of socket I/O.
///
/// Plaintext and encrypted (client and server) streams have different type signatures.
/// Exposing these types to the rest of the application is painful, so `Socket` provides
/// an opaque container for the various types of sockets supported by this proxy.
#[derive(Debug)]
pub struct Socket(Kind);

// Since the rustls types are much larger than the plain types, they are boxed. Because
// clippy says so.
enum Kind {
    Plain(TcpStream),
    SecureClient(Box<SecureStream<ClientSession>>),
    SecureServer(Box<SecureStream<ServerSession>>),
}

impl fmt::Debug for Kind {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Kind::Plain(ref s) => {
                f.debug_struct("Plain")
                    .field("peer", &s.peer_addr().unwrap())
                    .field("local", &s.local_addr().unwrap())
                    .finish()
            }
            Kind::SecureClient(ref s) => {
                f.debug_struct("SecureClient")
                    .field("peer", &s.peer_addr())
                    .field("local", &s.local_addr())
                    .finish()
            }
            Kind::SecureServer(ref s) => {
                f.debug_struct("SecureServer")
                    .field("peer", &s.peer_addr())
                    .field("local", &s.local_addr())
                    .finish()
            }
        }
    }
}

impl Socket {
    pub fn tcp_shutdown(&mut self, how: Shutdown) -> io::Result<()> {
        trace!("{:?}.tcp_shutdown({:?})", self, how);
        match self.0 {
            Kind::Plain(ref mut stream) => TcpStream::shutdown(stream, how),
            Kind::SecureClient(ref mut stream) => stream.tcp_shutdown(how),
            Kind::SecureServer(ref mut stream) => stream.tcp_shutdown(how),
        }
    }

    pub fn local_addr(&self) -> SocketAddr {
        match self.0 {
            Kind::Plain(ref stream) => stream.local_addr().unwrap(),
            Kind::SecureClient(ref stream) => stream.local_addr(),
            Kind::SecureServer(ref stream) => stream.local_addr(),
        }
    }

    pub fn peer_addr(&self) -> SocketAddr {
        match self.0 {
            Kind::Plain(ref stream) => stream.peer_addr().unwrap(),
            Kind::SecureClient(ref stream) => stream.peer_addr(),
            Kind::SecureServer(ref stream) => stream.peer_addr(),
        }
    }
}

/// Reads the socket without blocking.
impl Read for Socket {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        trace!("{:?}.read({})", self, buf.len());
        match self.0 {
            Kind::Plain(ref mut stream) => stream.read(buf),
            Kind::SecureClient(ref mut stream) => stream.read(buf),
            Kind::SecureServer(ref mut stream) => stream.read(buf),
        }
    }
}

/// Writes to the socket without blocking.
impl Write for Socket {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        trace!("{:?}.write({})", self, buf.len());
        match self.0 {
            Kind::Plain(ref mut stream) => stream.write(buf),
            Kind::SecureClient(ref mut stream) => stream.write(buf),
            Kind::SecureServer(ref mut stream) => stream.write(buf),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        trace!("{:?}.flush()", self);
        match self.0 {
            Kind::Plain(ref mut stream) => stream.flush(),
            Kind::SecureClient(ref mut stream) => stream.flush(),
            Kind::SecureServer(ref mut stream) => stream.flush(),
        }
    }
}

/// Closes the write-side of a stream.
impl AsyncWrite for Socket {
    fn shutdown(&mut self) -> Poll<(), io::Error> {
        trace!("{:?}.shutdown()", self);
        match self.0 {
            Kind::Plain(ref mut stream) => AsyncWrite::shutdown(stream),
            Kind::SecureClient(ref mut stream) => stream.shutdown(),
            Kind::SecureServer(ref mut stream) => stream.shutdown(),
        }
    }
}
