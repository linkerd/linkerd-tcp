use futures::{Async, Future, Poll, Sink, Stream};
use futures::sync::mpsc;
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
use tokio_core::reactor::{Core, Handle};

mod sni;
pub mod config;

use WeightedAddr;
use self::config::*;
use lb::{Balancer, Acceptor, Connector, PlainAcceptor, PlainConnector, SecureAcceptor,
         SecureConnector};
use namerd;
use self::sni::Sni;

const DEFAULT_BUFFER_SIZE: usize = 8 * 1024;
const DEFAULT_MAX_WAITERS: usize = 8;
const DEFAULT_NAMERD_SECONDS: u64 = 60;

/// Creates two reactor-aware runners from a configuration.
///
/// Each runner takes a Handle and produces a `Future`, which should be passed to `run`
/// which completes when the thread should stop running.
pub fn configure(app: AppConfig) -> (Admin, Proxies) {
    let transfer_buf = {
        let sz = app.buffer_size.unwrap_or(DEFAULT_BUFFER_SIZE);
        Rc::new(RefCell::new(vec![0;sz]))
    };

    let mut namerds = VecDeque::new();
    let mut proxies = VecDeque::new();
    let mut proxy_configs = app.proxies;
    for _ in 0..proxy_configs.len() {
        let ProxyConfig { namerd, servers, client, max_waiters, .. } = proxy_configs.pop().unwrap();
        let (tx, rx) = mpsc::channel(1);
        namerds.push_back(Namerd {
            config: namerd,
            sender: tx,
        });
        proxies.push_back(Proxy {
            addrs: Box::new(rx.fuse()),
            servers: servers,
            client: client,
            buf: transfer_buf.clone(),
            max_waiters: max_waiters.unwrap_or(DEFAULT_MAX_WAITERS),
        });
    }

    let admin = Admin { namerds: namerds };
    let proxies = Proxies { proxies: proxies };
    (admin, proxies)
}

pub trait Loader: Sized {
    type Future: Future<Item = (), Error = io::Error>;
    fn load(self, handle: Handle) -> io::Result<Self::Future>;
}
pub trait Runner: Sized {
    fn run(self) -> io::Result<()>;
}

impl<L: Loader> Runner for L {
    fn run(self) -> io::Result<()> {
        let mut core = Core::new()?;
        let fut = self.load(core.handle())?;
        core.run(fut)
    }
}

pub struct Admin {
    namerds: VecDeque<Namerd>,
}
impl Loader for Admin {
    type Future = Running;
    fn load(self, handle: Handle) -> io::Result<Running> {
        let mut running = Running::new();
        let mut namerds = self.namerds;
        for _ in 0..namerds.len() {
            let f = namerds.pop_front().unwrap().load(handle.clone())?;
            running.register(f.map_err(|_| io::ErrorKind::Other.into()));
        }
        Ok(running)
    }
}

struct Namerd {
    config: NamerdConfig,
    sender: mpsc::Sender<Vec<WeightedAddr>>,
}
impl Loader for Namerd {
    type Future = Box<Future<Item = (), Error = io::Error>>;
    fn load(self, handle: Handle) -> io::Result<Self::Future> {
        let path = self.config.path;
        let addr = self.config.addr;
        let interval = self.config
            .interval
            .unwrap_or_else(|| time::Duration::new(DEFAULT_NAMERD_SECONDS, 0));
        let ns = self.config.namespace.clone().unwrap_or_else(|| "default".into());
        info!("Updating {} in {} from {} every {}s",
              path,
              ns,
              addr,
              interval.as_secs());

        let client = Client::new(&handle);
        let addrs = namerd::resolve(client, self.config.addr, interval, &ns, &path)
            .map_err(|e| error!("namerd error: {:?}", e));
        let sink = self.sender.sink_map_err(|_| error!("sink error"));
        let driver = addrs.forward(sink)
            .map(|_| {})
            .map_err(|_| io::ErrorKind::Other.into());
        Ok(Box::new(driver))
    }
}

pub struct Proxies {
    proxies: VecDeque<Proxy>,
}
impl Loader for Proxies {
    type Future = Running;
    fn load(self, handle: Handle) -> io::Result<Running> {
        let mut running = Running::new();
        let mut proxies = self.proxies;
        for _ in 0..proxies.len() {
            let p = proxies.pop_front().unwrap();
            let f = p.load(handle.clone())?;
            running.register(f);
        }
        Ok(running)
    }
}

struct Proxy {
    servers: Vec<ServerConfig>,
    client: Option<ClientConfig>,
    addrs: Box<Stream<Item = Vec<WeightedAddr>, Error = ()>>,
    buf: Rc<RefCell<Vec<u8>>>,
    max_waiters: usize,
}
impl Loader for Proxy {
    type Future = Running;
    fn load(self, handle: Handle) -> io::Result<Running> {
        let client = self.client.and_then(|c| c.tls);
        match client {
            None => {
                run_balancer(&handle,
                             &self.servers,
                             self.addrs,
                             PlainConnector::new(handle.clone()),
                             self.buf,
                             self.max_waiters)
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
                run_balancer(&handle,
                             &self.servers,
                             self.addrs,
                             SecureConnector::new(c.name.clone(), tls, handle.clone()),
                             self.buf,
                             self.max_waiters)
            }
        }
    }
}

fn run_balancer<A, C>(handle: &Handle,
                      servers: &[ServerConfig],
                      addrs: A,
                      conn: C,
                      buf: Rc<RefCell<Vec<u8>>>,
                      max_waiters: usize)
                      -> io::Result<Running>
    where A: Stream<Item = Vec<WeightedAddr>, Error = ()> + 'static,
          C: Connector + 'static
{
    let addrs = addrs.map_err(|_| io::ErrorKind::Other.into());
    let bal = Balancer::new(addrs, conn, buf.clone()).into_shared(max_waiters, handle.clone());

    let mut running = Running::new();
    for s in servers {
        let handle = handle.clone();
        let bal = bal.clone();
        match *s {
            ServerConfig::Tcp { ref addr } => {
                let acceptor = PlainAcceptor::new(handle);
                let f = acceptor.accept(addr).forward(bal).map(|_| {});
                running.register(f);
            }
            ServerConfig::Tls { ref addr,
                                ref alpn_protocols,
                                ref default_identity,
                                ref identities,
                                .. } => {
                let mut tls = rustls::ServerConfig::new();
                tls.cert_resolver = load_cert_resolver(identities, default_identity);
                if let Some(ref protos) = *alpn_protocols {
                    tls.set_protocols(protos);
                }

                let acceptor = SecureAcceptor::new(handle, tls);
                let f = acceptor.accept(addr).forward(bal).map(|_| {});
                running.register(f);
            }
        }
    }
    Ok(running)
}

fn load_cert_resolver(ids: &Option<HashMap<String, TlsServerIdentity>>,
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

/// Tracks a list of `F`-typed `Future`s until are complete.
pub struct Running(VecDeque<Box<Future<Item = (), Error = io::Error>>>);
impl Running {
    fn new() -> Running {
        Running(VecDeque::new())
    }

    fn register<F>(&mut self, f: F)
        where F: Future<Item = (), Error = io::Error> + 'static
    {
        self.0.push_back(Box::new(f))
    }
}
impl Future for Running {
    type Item = ();
    type Error = io::Error;
    fn poll(&mut self) -> Poll<(), io::Error> {
        let sz = self.0.len();
        trace!("polling {} running", sz);
        for i in 0..sz {
            let mut f = self.0.pop_front().unwrap();
            trace!("polling runner {}", i);
            if f.poll()? == Async::NotReady {
                trace!("runner {} not ready", i);
                self.0.push_back(f);
            } else {
                trace!("runner {} finished", i);
            }
        }
        if self.0.is_empty() {
            trace!("runner finished");
            Ok(Async::Ready(()))
        } else {
            trace!("runner not finished");
            Ok(Async::NotReady)
        }
    }
}
