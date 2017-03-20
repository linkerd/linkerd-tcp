use futures::{Async, Future, Poll, Stream};
use hyper::Client;
use rustls;
use rustls::ResolvesServerCert;
use std::boxed::Box;
use std::cell::RefCell;
use std::collections::{VecDeque, HashMap};
use std::fs::File;
use std::io::{self, BufReader};
use std::rc::Rc;
use std::time;
use tokio_core::reactor::Handle;

mod sni;
pub mod config;

use WeightedAddr;
use self::config::*;
use lb::{Balancer, Acceptor, Connector, PlainAcceptor, PlainConnector, SecureAcceptor,
         SecureConnector, Shared, WithAddr};
use namerd;
use self::sni::Sni;

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
                self.configure_proxy_servers(p, handle, conn, buf);
            }
            Some(ref c) => {
                match c.tls {
                    None => {
                        let conn = PlainConnector::new(handle.clone());
                        self.configure_proxy_servers(p, handle, conn, buf);
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
                        self.configure_proxy_servers(p, handle, conn, buf);
                    }
                }
            }
        }
    }

    fn configure_proxy_servers<C>(&mut self,
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
            info!("Listening on {}", s.addr());
            self.configure_server(s, handle.clone(), balancer.clone());
        }
    }

    fn configure_server(&mut self, s: &ServerConfig, handle: Handle, balancer: Shared) {
        match *s {
            ServerConfig::Tcp { ref addr } => {
                let acceptor = PlainAcceptor::new(handle);
                let f = acceptor.accept(addr).forward(balancer);
                self.0.push_back(Box::new(f.map(|_| {})));
            }
            ServerConfig::Tls { ref addr,
                                ref alpn_protocols,
                                ref default_identity,
                                ref identities,
                                .. } => {
                let mut tls = rustls::ServerConfig::new();

                if let Some(ref protos) = *alpn_protocols {
                    tls.set_protocols(protos);
                }

                tls.cert_resolver = load_resolver(identities, default_identity);

                let acceptor = SecureAcceptor::new(handle, tls);
                let f = acceptor.accept(addr).forward(balancer);
                self.0.push_back(Box::new(f.map(|_| {})));
            }
        }
    }
}

fn load_resolver(ids: &Option<HashMap<String, TlsServerIdentity>>,
                 def: &Option<TlsServerIdentity>)
                 -> Box<ResolvesServerCert> {
    let mut is_empty = def.is_some();
    if let Some(ref ids) = *ids {
        is_empty = is_empty && ids.is_empty();
    }
    if is_empty {
        panic!("No TLS server identities specified");
    }

    Box::new(Sni::new(ids, def))
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
