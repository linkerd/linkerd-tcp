extern crate env_logger;
extern crate futures;
extern crate hyper;
extern crate tokio_core;
extern crate tokio_io;
extern crate linkerd_tcp;
extern crate tacho;

mod mocks;

use futures::Future;
use futures::stream::Stream;
use futures::sync::{mpsc, oneshot};
use hyper::server::Http;
use linkerd_tcp::app::{Loader, Namerd, Proxy, ProxyServer};
use linkerd_tcp::app::config::NamerdConfig;
use linkerd_tcp::app::config::ServerConfig::Tcp;
use mocks::MockNamerd;
use std::cell::RefCell;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::rc::Rc;
use std::thread;
use tacho::Tacho;
use tokio_core::net::TcpListener;
use tokio_core::reactor::Core;

#[test]
/// If namerd has no resolvable path then requests should fail fast.
fn empty_path() {
    drop(env_logger::init());

    let Tacho { metrics, aggregator, report } = Tacho::default();
    drop(aggregator);
    drop(report);

    let mut core = Core::new().unwrap();
    let handle = core.handle();

    let mut namerd_listener_addr: SocketAddr = "127.0.0.1:0"
        .parse()
        .expect("could not parse namerd_listener_addr");

    let namerd_listener = TcpListener::bind(&namerd_listener_addr, &handle.clone())
        .expect(format!("unable to bind mock namerd on address {:?}",
                        namerd_listener_addr)
            .as_str());
    namerd_listener_addr = namerd_listener.local_addr()
        .expect("could not get bound addr for namerd_listener");

    let mock_namerd_httpd = Http::new();
    let handle2 = handle.clone();
    let namerd_srv = namerd_listener.incoming()
        .map(move |(socket, addr)| {
            let server: MockNamerd = Default::default();
            mock_namerd_httpd.bind_connection(&handle2, socket, addr, server);
            namerd_listener_addr = addr;
            Ok(()) as Result<(), ()>
        })
        .collect();

    core.handle().spawn(namerd_srv.map(|_| {})
        .map_err(|_| {}));

    let namerd_config = NamerdConfig {
        url: format!("http://{}", namerd_listener_addr),
        path: "/svc/default".to_owned(),
        namespace: None,
        interval_secs: Some(5 as u64),
    };

    // Channel for connecting Namerd updates to the Proxy
    let (addrs_tx, addrs_rx) = mpsc::channel(1);

    let namerd = Namerd {
        config: namerd_config,
        sender: addrs_tx,
        metrics: metrics.clone(),
    };

    let handle2 = handle.clone();
    let (_, namerd) = namerd.load(handle2).expect("couldn't load() namerd.");

    let proxy_listener_addr: SocketAddr = "127.0.0.1:0"
        .parse()
        .unwrap();

    let buffer = Rc::new(RefCell::new(vec![0;1024]));

    let proxy = Proxy {
        client: None,
        server: ProxyServer {
            label: "proxy_server".to_owned(),
            servers: vec![Tcp { addr: proxy_listener_addr }],
            addrs: Box::new(addrs_rx.fuse()),
            buf: buffer,
            max_waiters: 1 as usize,
            metrics: metrics,
        },
    };

    let handle2 = handle.clone();
    let (proxy_local_addr, proxy) = proxy.load(handle2).expect("couldn't load() proxy.");

    core.handle().spawn(proxy.map(|_| {})
        .map_err(|_| {}));

    core.handle().spawn(namerd.map(|_| {})
        .map_err(|_| {}));

    let (c, p) = oneshot::channel::<()>();

    let _ = thread::spawn(move || {
        let mut client = TcpStream::connect(&proxy_local_addr)
            .expect(format!("unable to connect to proxy on {:?}", proxy_listener_addr).as_str());
        client.set_nodelay(true).expect("unable to set NODELAY");
        let request_with_no_path = "GET / HTTP/1.0\r\n\r\n".to_owned();
        let request_written = client.write(request_with_no_path.as_bytes());
        assert_eq!(request_written.expect("bytes not written"),
                   request_with_no_path.as_str().len());
        let mut reader_buf = vec![0 as u8; 16 * 1024];
        let response_read = client.read_to_end(&mut reader_buf);
        match response_read {
            Ok(bytes) => assert_eq!(bytes, 0), // Either we read 0 bytes
            Err(_) => assert!(true), // Or we got an error.
        };
        c.send(()).expect("could not send");
    });

    core.run(p).expect("our oneshot cannot be completed.");
}