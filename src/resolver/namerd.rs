//! Namerd Endpointer

// TODO In the future, we likely want to change this to use the split bind & addr APIs so
// balancers can be shared across logical names. In the meantime, it's sufficient to have
// a balancer per logical name.

use super::{DstAddr, Result, Error};
use bytes::{Buf, BufMut, IntoBuf, Bytes, BytesMut};
use futures::{Async, Future, IntoFuture, Poll, Stream, future};
use hyper::{Body, Chunk, Client};
use hyper::client::{Connect as HyperConnect, HttpConnector};
use hyper::status::StatusCode;
use serde_json as json;
use std::{f32, net, time};
use std::collections::HashMap;
use std::rc::Rc;
use tacho::{self, Timing};
use tokio_core::reactor::Handle;
use tokio_timer::{Timer, Interval};
use url::Url;

type HttpConnectorFactory = Client<HttpConnector>;

type AddrsFuture = Box<Future<Item = Vec<DstAddr>, Error = Error>>;

// pub struct Addrs(Box<Stream<Item = Result<Vec<DstAddr>>, Error = ()>>);
// impl Stream for Addrs {
//     type Item = Result<Vec<DstAddr>>;
//     type Error = ();
//     fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
//         self.0.poll()
//     }
// }

#[derive(Clone)]
pub struct Namerd {
    base_url: String,
    period: time::Duration,
    namespace: String,
    stats: Stats,
}

impl Namerd {
    pub fn new(base_url: String,
               period: time::Duration,
               namespace: String,
               metrics: tacho::Scope)
               -> Namerd {
        Namerd {
            base_url: format!("{}/api/1/resolve/{}", base_url, namespace),
            stats: Stats::new(metrics),
            namespace,
            period,
        }
    }
}

impl Namerd {
    pub fn with_client(self, handle: &Handle, timer: &Timer) -> WithClient {
        WithClient {
            namerd: self,
            client: Rc::new(Client::new(handle)),
            timer: timer.clone(),
        }
    }
}

pub struct WithClient {
    namerd: Namerd,
    client: Rc<HttpConnectorFactory>,
    timer: Timer,
}
impl WithClient {
    pub fn resolve(&self, target: &str) -> Addrs {
        let url = Url::parse_with_params(&self.namerd.base_url, &[("path", &target)])
            .expect("invalid namerd url");
        let init = request(self.client.clone(), url.clone(), self.namerd.stats.clone());
        let interval = self.timer.interval(self.namerd.period);
        Addrs {
            client: self.client.clone(),
            stats: self.namerd.stats.clone(),
            state: Some(State::Pending(init, interval)),
            url,
        }
    }
}

pub struct Addrs {
    state: Option<State>,
    client: Rc<HttpConnectorFactory>,
    url: Url,
    stats: Stats,
}

enum State {
    Pending(AddrsFuture, Interval),
    Waiting(Interval),
}

impl Stream for Addrs {
    type Item = Result<Vec<DstAddr>>;
    type Error = Error;
    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        loop {
            match self.state.take().expect("polled after completion") {
                State::Waiting(mut int) => {
                    match int.poll() {
                        Err(e) => {
                            self.state = Some(State::Waiting(int));
                            return Err(Error::Timer(e));
                        }
                        Ok(Async::NotReady) => {
                            self.state = Some(State::Waiting(int));
                            return Ok(Async::NotReady);
                        }
                        Ok(Async::Ready(_)) => {
                            let fut = {
                                let c = self.client.clone();
                                let u = self.url.clone();
                                let s = self.stats.clone();
                                request(c, u, s)
                            };
                            self.state = Some(State::Pending(fut, int));
                        }
                    }
                }
                State::Pending(mut fut, int) => {
                    match fut.poll() {
                        Err(e) => {
                            self.state = Some(State::Waiting(int));
                            return Ok(Async::Ready(Some(Err(e))));
                        }
                        Ok(Async::Ready(addrs)) => {
                            self.state = Some(State::Waiting(int));
                            return Ok(Async::Ready(Some(Ok(addrs))));
                        }
                        Ok(Async::NotReady) => {
                            self.state = Some(State::Pending(fut, int));
                            return Ok(Async::NotReady);
                        }
                    }
                }
            }
        }
    }
}

fn request<C: HyperConnect>(client: Rc<Client<C>>, url: Url, stats: Stats) -> AddrsFuture {
    debug!("Polling namerd at {}", url.to_string());
    let mut stats = stats;
    let rsp = future::lazy(|| Ok(tacho::Timing::start())).and_then(move |start_t| {
        client
            .get(url)
            .then(handle_response)
            .then(move |rsp| {
                stats.request_latency_ms.add(start_t.elapsed_ms());
                if rsp.is_ok() {
                    stats.success_count.incr(1);
                } else {
                    stats.failure_count.incr(1);
                }
                rsp
            })
    });
    Box::new(rsp)
}

fn handle_response(result: ::hyper::Result<::hyper::client::Response>) -> AddrsFuture {
    match result {
        Ok(rsp) => {
            match *rsp.status() {
                StatusCode::Ok => parse_body(rsp.body()),
                status => {
                    info!("error: bad response: {}", status);
                    Box::new(Err(Error::UnexpectedStatus(status)).into_future())
                }
            }
        }
        Err(e) => {
            error!("failed to read response: {:?}", e);
            Box::new(Err(Error::Hyper(e)).into_future())
        }
    }
}

fn parse_body(body: Body) -> AddrsFuture {
    trace!("parsing namerd response");
    body.collect()
        .then(|res| match res {
                  Ok(ref chunks) => parse_chunks(chunks),
                  Err(e) => {
                      info!("error: {}", e);
                      Err(Error::Hyper(e))
                  }
              })
        .boxed()
}

fn bytes_in(chunks: &[Chunk]) -> usize {
    let mut sz = 0;
    for c in chunks {
        sz += (*c).len();
    }
    sz
}

fn to_buf(chunks: &[Chunk]) -> Bytes {
    let mut buf = BytesMut::with_capacity(bytes_in(chunks));
    for c in chunks {
        buf.put_slice(&*c)
    }
    buf.freeze()
}

fn parse_chunks(chunks: &[Chunk]) -> Result<Vec<DstAddr>> {
    let r = to_buf(chunks).into_buf().reader();
    let result: json::Result<NamerdResponse> = json::from_reader(r);
    match result {
        Ok(ref nrsp) if nrsp.kind == "bound" => Ok(to_weighted_addrs(&nrsp.addrs)),
        Ok(_) => Err(Error::NotBound),
        Err(e) => {
            info!("error parsing response: {}", e);
            Err(Error::Serde(e))
        }
    }
}

fn to_weighted_addrs(namerd_addrs: &[NamerdAddr]) -> Vec<DstAddr> {
    // We never intentionally clear the EndpointMap.
    let mut weighted_addrs: Vec<DstAddr> = Vec::new();
    for na in namerd_addrs {
        let addr = net::SocketAddr::new(na.ip.parse().unwrap(), na.port);
        let w = na.meta.endpoint_addr_weight.unwrap_or(1.0);
        weighted_addrs.push(DstAddr::new(addr, w));
    }
    weighted_addrs
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


#[derive(Clone)]
pub struct Stats {
    request_latency_ms: tacho::Stat,
    success_count: tacho::Counter,
    failure_count: tacho::Counter,
}
impl Stats {
    fn new(metrics: tacho::Scope) -> Stats {
        let metrics = metrics.labeled("service".into(), "namerd".into());
        Stats {
            request_latency_ms: metrics.stat("namerd_request_latency_ms".into()),
            success_count: metrics.counter("namerd_success_count".into()),
            failure_count: metrics.counter("namerd_failure_count".into()),
        }
    }
}
