#[macro_use]
extern crate clap;
extern crate futures;
extern crate linkerd_tcp;
#[macro_use]
extern crate log;
extern crate pretty_env_logger;
extern crate tokio_core;

use clap::{Arg, App};
use futures::sync::oneshot;
use linkerd_tcp::app::{AppConfig, AppSpawner};
use std::fs;
use std::io::Read;
use std::thread;
use tokio_core::reactor::Core;

static CONFIG_PATH_ARG: &'static str = "PATH";

fn main() {
    // Configure the logger from the RUST_LOG environment variable.
    drop(pretty_env_logger::init());

    // Load command-line options.
    let opts = App::new(crate_name!())
        .version(crate_version!())
        .about(crate_description!())
        .arg(Arg::with_name(CONFIG_PATH_ARG)
                 .required(true)
                 .index(1)
                 .help("Config file path."))
        .get_matches();

    // Parse configuration file.
    let config: AppConfig = {
        let path = opts.value_of(CONFIG_PATH_ARG).unwrap();
        let mut txt = String::new();
        fs::File::open(path)
            .and_then(|mut f| f.read_to_string(&mut txt))
            .expect("failed to read config");
        txt.parse().expect("configuration error")
    };

    let core = Core::new().expect("failed to initialize admin reactor");
    let remote = core.remote();
    let (close_tx, close_rx) = oneshot::channel::<()>();

    // Process the configuration, splitting it into two threads. These threads are
    // connected by synchronization primitives as needed, but no work is being done yet.
    // Next, we'll attach each of these to a reactor in an independent thread, driving
    // both admin and serving work.
    let AppSpawner { routers, admin } = config.into_app().expect("failed to load configuration");

    // Run the servers in a new thread.
    let serve_thread = thread::Builder::new()
        .name("serve".into())
        .spawn(move || {
                   let mut core = Core::new().expect("failed to initialize server reactor");
                   let mut routers = routers;
                   for router in routers.drain(..) {
                       router.spawn(core.handle(), remote.clone());
                   }
                   core.run(close_rx).expect("failed to run")
               })
        .expect("could not spawn admin thread");

    admin
        .run(close_tx, core)
        .expect("failed to run the admin server");

    serve_thread
        .join()
        .expect("failed to wait for serve thread");

    info!("Closing.")
}
