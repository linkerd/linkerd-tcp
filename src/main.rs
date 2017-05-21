#[macro_use]
extern crate clap;
extern crate futures;
extern crate linkerd_tcp;
#[macro_use]
extern crate log;
extern crate pretty_env_logger;
extern crate tokio_core;
extern crate tokio_timer;

use clap::{Arg, App};
use linkerd_tcp::app::{self, AppConfig, AppSpawner};
use std::fs;
use std::io::Read;
use std::thread;
use tokio_core::reactor::Core;
use tokio_timer::Timer;

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
    debug!("parsed config: {:?}", config);

    let mut core = Core::new().expect("failed to initialize admin reactor");

    // Process the configuration, splitting it into two threads. These threads are
    // connected by synchronization primitives as needed, but no work is being done yet.
    // Next, we'll attach each of these to a reactor in an independent thread, driving
    // both admin and serving work.
    let AppSpawner { mut routers, admin } =
        config.into_app().expect("failed to load configuration");
    debug!("loaded app");

    let (closer, closed) = app::closer();

    // One timer is created for the whole process.
    let timer = Timer::default();

    // Create a background admin thread that:
    // - runs an admin server;
    // - executes namerd resolutions;
    let admin_thread = {
        let timer = timer.clone();
        thread::Builder::new()
            .name("admin".into())
            .spawn(move || {
                       let mut core = Core::new().expect("failed to initialize admin reactor");
                       debug!("running admin server");
                       admin
                           .run(closer, &mut core, &timer)
                           .expect("failed to run the admin server");
                   })
            .expect("could not spawn admin thread")
    };

    // Schedule all routers on the main thread.
    let handle = core.handle();
    while let Some(r) = routers.pop_front() {
        debug!("spawning router");
        r.spawn(&handle, &timer).expect("failed to spawn router");
    }

    // Run until the admin thread closes the application.
    debug!("running until admin server closes");
    core.run(closed).expect("failed to run");

    // Wait until the admin thread completes.
    admin_thread
        .join()
        .expect("failed to wait for serve thread");

    debug!("Closed.")
}
