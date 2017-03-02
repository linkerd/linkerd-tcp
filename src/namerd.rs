//! Namerd Endpointer

use hyper::Client;
use hyper::status::StatusCode;
use rand::{Rng, thread_rng};
use std::collections::{HashSet, HashMap};
use std::f32;
use std::net::SocketAddr;
use std::time::Duration;
use serde_json as json;
use std::thread;
use std::sync::{Arc, RwLock};
use std::sync::atomic::{self, AtomicBool};
use url::Url;

#[derive(Debug, PartialEq)]
struct EndpointState {
    weight: f32,
    load: f32,
}

type EndpointMap = Arc<RwLock<HashMap<SocketAddr, Arc<RwLock<EndpointState>>>>>;

pub struct Endpointer {
    is_running: Arc<AtomicBool>,
    #[allow(dead_code)]
    thread: thread::JoinHandle<()>,
    endpoints: EndpointMap,
}

impl Endpointer {
    pub fn periodic(addr: &SocketAddr,
                    period: Duration,
                    namespace: String,
                    target: String)
                    -> Endpointer {
        let is_running = Arc::new(AtomicBool::new(true));
        let endpoints: EndpointMap = Arc::new(RwLock::new(HashMap::new()));

        let name = format!("namerd-{}-{}-{}",
                           addr.ip(),
                           addr.port().to_string(),
                           target);

        let base = format!("http://{}:{}/api/1/resolve/{}",
                           addr.ip(),
                           addr.port().to_string(),
                           namespace);

        let thread = {
            let is_running = is_running.clone();
            let endpoints = endpoints.clone();
            let url = Url::parse_with_params(&base, &[("path", &target)]).unwrap();
            thread::Builder::new()
                .name(name)
                .spawn(move || {
                    let client = Client::new();
                    while is_running.load(atomic::Ordering::Relaxed) {
                        match client.get(url.clone()).send() {
                            Err(e) => warn!("Error fetching addresses: {}", e),
                            Ok(rsp) => {
                                if rsp.status != StatusCode::Ok {
                                    warn!("Failed to fetch addresses: {}", rsp.status);
                                } else {
                                    let r: json::Result<NamerdResponse> = json::from_reader(rsp);
                                    match r {
                                        Err(e) => warn!("Failed to parse response: {}", e),
                                        Ok(rsp) => update(rsp.addrs, endpoints.clone()),
                                    }
                                }
                            }
                        }
                        thread::sleep(period);
                    }
                })
                .unwrap()
        };
        Endpointer {
            is_running: is_running,
            thread: thread,
            endpoints: endpoints,
        }
    }
}

fn update(namerd_addrs: Vec<NamerdAddr>, endpoints: EndpointMap) {
    let weights = {
        let mut weights = HashMap::new();
        for na in namerd_addrs.iter() {
            let ip = na.ip.parse().unwrap();
            let w = na.meta.endpoint_addr_weight.unwrap_or(1.0);
            weights.insert(SocketAddr::new(ip, na.port), w);
        }
        weights
    };
    let addrs = weights.keys().collect::<HashSet<&SocketAddr>>();

    let mut eps = endpoints.write().unwrap();

    // Figure out which endpoints have been removed.
    let rm_keys = eps.keys()
        .filter_map(|&a| if addrs.contains(&a) {
            None
        } else {
            Some(a.clone())
        })
        .collect::<HashSet<SocketAddr>>();

    // Then, remove them.
    for k in rm_keys.iter() {
        trace!("removing {:?}", k);
        eps.remove(&k);
    }

    // Finally, add new endpoints.
    for addr in addrs.iter() {
        let weight = weights[*addr];
        if eps.contains_key(addr) {
            trace!("Updating {} *{}", addr, weight);
            let mut s = eps.get(addr).unwrap().write().unwrap();
            s.weight = weight;

        } else {
            trace!("Adding {} *{}", addr, weight);
            let state = Arc::new(RwLock::new(EndpointState {
                weight: weight,
                load: 0.0,
            }));
            eps.insert(**addr, state);
        }
    }
}

impl Drop for Endpointer {
    fn drop(&mut self) {
        info!("shutting down namerd");
        self.is_running.store(false, atomic::Ordering::SeqCst);
        // self.thread.join().unwrap();
    }
}

pub struct Endpoint {
    addr: SocketAddr,
    state: Arc<RwLock<EndpointState>>,
}

impl ::WithAddr for Endpoint {
    fn addr(&self) -> SocketAddr {
        self.addr.clone()
    }
}

impl ::Endpointer for Endpointer {
    type Endpoint = Endpoint;

    fn endpoint(&self) -> Option<Endpoint> {
        let endpoints = self.endpoints.read().unwrap();

        let addrs: Vec<(&SocketAddr, &Arc<RwLock<EndpointState>>)> = endpoints.iter().collect();
        match addrs.len() {
            0 => None,
            n => {
                let (addr, state) = if n == 1 {
                    let (addr0, state0) = addrs[0];
                    (addr0.clone(), (*state0).clone())
                } else {
                    // Pick two distinct endpoints at random.
                    let (i0, i1) = {
                        let mut rng = thread_rng();
                        let i0 = rng.gen_range(0, n);
                        let mut i1 = i0;
                        while i0 == i1 {
                            i1 = rng.gen_range(0, n);
                        }
                        (i0, i1)
                    };
                    let (addr0, state0) = addrs[i0];
                    let (addr1, state1) = addrs[i1];

                    let s0 = (*state0).read().unwrap();
                    let s1 = (*state1).read().unwrap();
                    trace!("Choosing between {} *{} @{} and {} *{} @{}",
                           addr0,
                           s0.weight,
                           s0.load,
                           addr1,
                           s1.weight,
                           s1.load);

                    let load0 = weighted_load(s0.load, s0.weight);
                    let load1 = weighted_load(s1.load, s1.weight);
                    if load0 < load1 {
                        (addr0.clone(), (*state0).clone())
                    } else {
                        (addr1.clone(), (*state1).clone())
                    }
                };

                // XXX currently we use a simple load metric (# active
                // conns).  This load metric should be pluggable to
                // account for throughput, etc.
                {
                    let mut state = state.write().unwrap();
                    state.load += 1.0;
                    trace!("Using {} {:?}", addr, *state);
                }

                Some(Endpoint {
                    addr: addr,
                    state: state,
                })
            }
        }
    }
}

/// The higher the weight, the lower the load.
fn weighted_load(load: f32, weight: f32) -> f32 {
    if weight == 0.0 {
        f32::INFINITY
    } else {
        load / weight
    }
}

impl Drop for Endpoint {
    fn drop(&mut self) {
        let mut state = self.state.write().unwrap();
        trace!("releasing endpoint: {:?}", *state);
        assert!(state.load >= 1.0);
        state.load -= 1.0;
    }
}

#[derive(Debug, Deserialize)]
struct NamerdResponse {
    #[serde(rename = "type")]
    kind: String,
    addrs: Vec<NamerdAddr>,
    meta: HashMap<String, String>,
}

#[derive(Debug, Deserialize)]
struct NamerdAddr {
    ip: String,
    port: u16,
    meta: Meta,
}

#[derive(Debug, Deserialize)]
struct Meta {
    authority: Option<String>,

    #[serde(rename = "nodeName")]
    node_name: Option<String>,

    endpoint_addr_weight: Option<f32>,
}
