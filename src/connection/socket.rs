use super::secure::SecureStream;
use futures::Poll;
use rustls::{ClientSession, ServerSession};
use std::fmt;
use std::io::{self, Read, Write};
use std::net::{Shutdown, SocketAddr};
use tokio_core::net::TcpStream;
use tokio_io::AsyncWrite;

pub fn plain(tcp: TcpStream) -> Socket {
    Socket {
        local_addr: tcp.local_addr().expect("tcp stream has no local address"),
        peer_addr: tcp.peer_addr().expect("tcp stream has no peer address"),
        kind: Kind::Plain(tcp),
    }
}

pub fn secure_client(tls: SecureStream<ClientSession>) -> Socket {
    Socket {
        local_addr: tls.local_addr(),
        peer_addr: tls.peer_addr(),
        kind: Kind::SecureClient(Box::new(tls)),
    }
}

pub fn secure_server(tls: SecureStream<ServerSession>) -> Socket {
    Socket {
        local_addr: tls.local_addr(),
        peer_addr: tls.peer_addr(),
        kind: Kind::SecureServer(Box::new(tls)),
    }
}

/// Hides the implementation details of socket I/O.
///
/// Plaintext and encrypted (client and server) streams have different type signatures.
/// Exposing these types to the rest of the application is painful, so `Socket` provides
/// an opaque container for the various types of sockets supported by this proxy.
pub struct Socket {
    local_addr: SocketAddr,
    peer_addr: SocketAddr,
    kind: Kind,
}

// Since the rustls types are much larger than the plain type, they are boxed. Because
// clippy says so.
enum Kind {
    Plain(TcpStream),
    SecureClient(Box<SecureStream<ClientSession>>),
    SecureServer(Box<SecureStream<ServerSession>>),
}

impl fmt::Debug for Socket {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self.kind {
            Kind::Plain(_) => {
                f.debug_struct("Plain")
                    .field("peer", &self.peer_addr)
                    .field("local", &self.local_addr)
                    .finish()
            }
            Kind::SecureClient(_) => {
                f.debug_struct("SecureClient")
                    .field("peer", &self.peer_addr)
                    .field("local", &self.local_addr)
                    .finish()
            }
            Kind::SecureServer(_) => {
                f.debug_struct("SecureServer")
                    .field("peer", &self.peer_addr)
                    .field("local", &self.local_addr)
                    .finish()
            }
        }
    }
}

impl Socket {
    pub fn tcp_shutdown(&mut self, how: Shutdown) -> io::Result<()> {
        trace!("{:?}.tcp_shutdown({:?})", self, how);
        match self.kind {
            Kind::Plain(ref mut stream) => TcpStream::shutdown(stream, how),
            Kind::SecureClient(ref mut stream) => stream.tcp_shutdown(how),
            Kind::SecureServer(ref mut stream) => stream.tcp_shutdown(how),
        }
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    pub fn peer_addr(&self) -> SocketAddr {
        self.peer_addr
    }
}

/// Reads the socket without blocking.
impl Read for Socket {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let ret = match self.kind {
            Kind::Plain(ref mut stream) => stream.read(buf),
            Kind::SecureClient(ref mut stream) => stream.read(buf),
            Kind::SecureServer(ref mut stream) => stream.read(buf),
        };
        trace!("{:?}.read({}) -> {:?}", self, buf.len(), ret);
        ret
    }
}

/// Writes to the socket without blocking.
impl Write for Socket {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let ret = match self.kind {
            Kind::Plain(ref mut stream) => stream.write(buf),
            Kind::SecureClient(ref mut stream) => stream.write(buf),
            Kind::SecureServer(ref mut stream) => stream.write(buf),
        };
        trace!("{:?}.write({}) -> {:?}", self, buf.len(), ret);
        ret
    }

    fn flush(&mut self) -> io::Result<()> {
        trace!("{:?}.flush()", self);
        match self.kind {
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
        match self.kind {
            Kind::Plain(ref mut stream) => AsyncWrite::shutdown(stream),
            Kind::SecureClient(ref mut stream) => stream.shutdown(),
            Kind::SecureServer(ref mut stream) => stream.shutdown(),
        }
    }
}
