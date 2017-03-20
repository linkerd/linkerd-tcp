use futures::{Async, Future, Poll, Stream};
use hyper::Client;
use rustls;
use std::boxed::Box;
use std::cell::RefCell;
use std::collections::VecDeque;
use std::fs::File;
use std::io::{self, BufReader};
use std::rc::Rc;
use std::time;
use tokio_core::reactor::Handle;

pub mod config;

use WeightedAddr;
use self::config::*;
use namerd;
use lb::{Balancer, Acceptor, Connector, PlainAcceptor, PlainConnector, SecureAcceptor,
         SecureConnector};

const DEFAULT_BUFFER_SIZE: usize = 8 * 1024;
const DEFAULT_MAX_WAITERS: usize = 8;
const DEFAULT_NAMERD_SECONDS: u64 = 60;

///
// TODO use another error type that indicates config error more clearly.
pub fn run(config_str: &str,
           handle: &Handle)
           -> io::Result<Box<Future<Item = (), Error = io::Error>>> {
    let app = config::from_str(config_str)?;
    let mut runner = Runner::new();

    let transfer_buf = {
        let sz = app.buffer_size.unwrap_or(DEFAULT_BUFFER_SIZE);
        Rc::new(RefCell::new(vec![0;sz]))
    };

    for proxy in app.proxies {
        runner.configure_proxy(&proxy, handle.clone(), transfer_buf.clone());
    }

    Ok(Box::new(runner))
}

fn mk_addrs(proxy: &ProxyConfig,
            handle: &Handle)
            -> Box<Stream<Item = Vec<WeightedAddr>, Error = io::Error>> {
    let n = &proxy.namerd;
    let interval = n.interval
        .unwrap_or_else(|| time::Duration::new(DEFAULT_NAMERD_SECONDS, 0));
    let ns = n.namespace.clone().unwrap_or_else(|| "default".into());
    info!("Updating {} in {} from {} every {}s",
          n.path,
          ns,
          n.addr,
          interval.as_secs());

    let client = Client::new(handle);
    let s = namerd::resolve(client, n.addr, interval, &ns, &n.path)
        .map_err(|_| io::Error::new(io::ErrorKind::Other, "namerd error"));
    Box::new(s)
}

/// Tracks a list of `F`-typed `Future`s until are complete.
pub struct Runner(VecDeque<Box<Future<Item = (), Error = io::Error>>>);

impl Runner {
    fn new() -> Runner {
        Runner(VecDeque::new())
    }

    fn configure_proxy(&mut self, p: &ProxyConfig, handle: Handle, buf: Rc<RefCell<Vec<u8>>>) {
        match p.client {
            None => {
                let conn = PlainConnector::new(handle.clone());
                self.configure_with_connector(p, handle, conn, buf);
            }
            Some(ref c) => {
                match c.tls {
                    None => {
                        let conn = PlainConnector::new(handle.clone());
                        self.configure_with_connector(p, handle, conn, buf);
                    }
                    Some(ref c) => {
                        let mut tls = rustls::ClientConfig::new();
                        if let Some(ref certs) = c.trust_cert_paths {
                            for p in certs {
                                let f = File::open(p).expect("cannot open certificate file");
                                tls.root_store
                                    .add_pem_file(&mut BufReader::new(f))
                                    .expect("certificate error");
                            }
                        };
                        let conn = SecureConnector::new(c.name.clone(), tls, handle.clone());
                        self.configure_with_connector(p, handle, conn, buf);
                    }
                }
            }
        }
    }

    fn configure_with_connector<C>(&mut self,
                                   p: &ProxyConfig,
                                   handle: Handle,
                                   conn: C,
                                   buf: Rc<RefCell<Vec<u8>>>)
        where C: Connector + 'static
    {
        let addrs = mk_addrs(p, &handle);

        let balancer = {
            let maxw = p.max_waiters.unwrap_or(DEFAULT_MAX_WAITERS);
            let bal = Balancer::new(addrs, conn, buf.clone());
            bal.into_shared(maxw, handle.clone())
        };

        for s in &p.servers {
            info!("Listening on {}", s.addr);
            match s.tls {
                None => {
                    let acceptor = PlainAcceptor::new(handle.clone());
                    let f = acceptor.accept(&s.addr).forward(balancer.clone());
                    self.0.push_back(Box::new(f.map(|_| {})));
                }
                Some(ref c) => {
                    let mut tls = rustls::ServerConfig::new();
                    if let Some(ref protos) = c.alpn_protocols {
                        tls.set_protocols(protos);
                    }

                    let certs = {
                        let mut certs = vec![];
                        for p in &c.cert_paths {
                            certs.append(&mut load_certs(p));
                        }
                        certs
                    };
                    let private_key = load_private_key(&c.private_key_path);
                    tls.set_single_cert(certs, private_key);

                    let acceptor = SecureAcceptor::new(handle.clone(), tls);
                    let f = acceptor.accept(&s.addr).forward(balancer.clone());
                    self.0.push_back(Box::new(f.map(|_| {})));
                }
            }
        }
    }
}

// from rustls example
fn load_certs(filename: &str) -> Vec<rustls::Certificate> {
    let certfile = File::open(filename).expect("cannot open certificate file");
    let mut r = BufReader::new(certfile);
    rustls::internal::pemfile::certs(&mut r).unwrap()
}

// from rustls example
fn load_private_key(filename: &str) -> rustls::PrivateKey {
    let keyfile = File::open(filename).expect("cannot open private key file");
    let mut r = BufReader::new(keyfile);
    let keys = rustls::internal::pemfile::rsa_private_keys(&mut r).unwrap();
    assert_eq!(keys.len(), 1);
    keys[0].clone()
}

impl Future for Runner {
    type Item = ();
    type Error = io::Error;
    fn poll(&mut self) -> Poll<(), io::Error> {
        let sz = self.0.len();
        for _ in 0..sz {
            let mut f = self.0.pop_front().unwrap();
            if f.poll()? == Async::NotReady {
                self.0.push_back(f);
            }
        }
        if self.0.is_empty() {
            Ok(Async::Ready(()))
        } else {
            Ok(Async::NotReady)
        }
    }
}