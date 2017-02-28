//! A simple Layer 4 proxy. Currently TCP only.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::env;
use std::io;
use std::net::SocketAddr;
use std::sync::{Arc, atomic, RwLock};
use std::thread;
use std::time::Duration;

extern crate rand;
use rand::{Rng, thread_rng};

#[macro_use]
extern crate log;
extern crate env_logger;

#[macro_use]
extern crate futures;
use futures::{Future, Stream};

extern crate tokio_core;
use tokio_core::reactor::Core;
use tokio_core::net::{TcpListener, TcpStream};

extern crate galadriel;
use galadriel::transfer::BufferedTransfer;

const WINDOW_SIZE: usize = 64 * 1024;

fn main() {
    drop(env_logger::init());

    // The source addr for clients to connect to. we can either get it
    // from the environment (ARGV[1]) or use a default
    let listen_addr = parse_addr(env::args().nth(1).unwrap_or("0.0.0.0:7575".to_string()));
    let namerd_addr = parse_addr(env::args().nth(2).unwrap_or("127.0.0.1:4180".to_string()));

    // Create the event loop and TCP listener we'll accept connections on.
    let mut core = Core::new().unwrap();
    let handle = core.handle();

    let namerd = {
        let down_addr = parse_addr(env::args().nth(2).unwrap_or("127.0.0.1:8080".to_string()));
        Arc::new(NamerdLookup::periodic(namerd_addr, Duration::from_secs(3), down_addr))
    };

    let proxy = {
        let buffer = Rc::new(RefCell::new(vec![0; WINDOW_SIZE]));

        let listener = TcpListener::bind(&listen_addr, &handle).unwrap();
        info!("Listening on {}", listen_addr);

        let get_addr = || {
            let namerd = namerd.clone();
            let sample: Rc<HashMap<SocketAddr, f32>> = namerd.sample().clone();
            let keys = sample.keys();
            let addrs: Vec<&SocketAddr> = keys.collect();
            let mut rng = thread_rng();
            let addr = *rng.choose(&addrs).unwrap();
            *addr
        };

        listener.incoming().for_each(move |(up_stream, up_addr)| {
            let down_addr = get_addr();

            debug!("Proxying {} to {}", up_addr, down_addr);
            let connect = TcpStream::connect(&down_addr, &handle);
            let tx = {
                let buffer = buffer.clone();
                connect.and_then(|down_stream| transmit_duplex(up_stream, down_stream, buffer))
            };
            handle.spawn(tx.then(move |res| {
                match res {
                    Err(e) => error!("Error proxying {} to {}: {}", up_addr, down_addr, e),
                    Ok((d, u)) => {
                        debug!("Proxied {} to {}: down={}B up={}B",
                               up_addr,
                               down_addr,
                               d,
                               u)
                    }
                };
                Ok(())
            }));
            Ok(())
        })
    };

    // You can run multiple `core.run()` in multiple threads, and use
    // SO_REUSEPORT tokio does not currently support thread pool
    // executors but you can thread.spawn N times.
    core.run(proxy).unwrap();
    // namerd.close();
}

fn parse_addr(a: String) -> SocketAddr {
    match a.parse() {
        Err(err) => panic!("unable to parse '{}' due to: {}.", a, err),
        Ok(addr) => addr,
    }
}

fn transmit_duplex(up_stream: TcpStream,
                   down_stream: TcpStream,
                   buffer: Rc<RefCell<Vec<u8>>>)
                   -> Box<Future<Item = (u64, u64), Error = io::Error>> {
    let up = Rc::new(up_stream);
    let down = Rc::new(down_stream);
    let down_tx = BufferedTransfer::new(up.clone(), down.clone(), buffer.clone());
    let up_tx = BufferedTransfer::new(down, up, buffer.clone());
    Box::new(down_tx.join(up_tx))
}

// struct Endpoint(SocketAddr);

// trait Endpointer {
//     fn get() -> Endpoint;
// }

struct NamerdLookup {
    is_running: Arc<atomic::AtomicBool>,
    thread: Rc<RefCell<thread::JoinHandle<()>>>,
    addr_weights: Arc<RwLock<HashMap<SocketAddr, f32>>>,
}

impl NamerdLookup {
    fn periodic(addr: SocketAddr, period: Duration, xxx_addr: SocketAddr) -> NamerdLookup {
        let name = format!("namerd-{}-{}", addr.ip(), addr.port());
        let is_running = Arc::new(atomic::AtomicBool::new(true));
        let addr_weights = {
            let mut map = HashMap::new();
            map.insert(xxx_addr, 1.0 as f32);
            Arc::new(RwLock::new(map))
        };
        let thread = {
            let is_running = is_running.clone();
            let addr_weights = addr_weights.clone();
            thread::Builder::new()
                .name(name.to_string())
                .spawn(move || while is_running.load(atomic::Ordering::Relaxed) {
                    info!("TODO hit the resolve api at {}...", addr);
                    drop(addr_weights.write().unwrap());
                    thread::sleep(period);
                })
                .unwrap()
        };
        NamerdLookup {
            addr_weights: addr_weights,
            is_running: is_running,
            thread: Rc::new(RefCell::new(thread)),
        }
    }

    fn sample(&self) -> Rc<HashMap<SocketAddr, f32>> {
        Rc::new(self.addr_weights.read().unwrap().clone())
    }
}

impl Drop for NamerdLookup {
    fn drop(&mut self) {
        self.is_running.store(false, atomic::Ordering::SeqCst);
        // self.thread.into_inner().join().unwrap();
    }
}
