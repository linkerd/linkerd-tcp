#[macro_use]
extern crate clap;
extern crate futures;
extern crate linkerd_tcp;
#[macro_use]
extern crate log;
extern crate pretty_env_logger;
extern crate tokio_core;
extern crate tokio_timer;

use clap::{Arg, App as ClapApp};
use linkerd_tcp::app::{self, AppConfig, App, AdminRunner, RouterSpawner};
use std::collections::VecDeque;
use std::fs;
use std::io::Read;
use std::thread;
use tokio_core::reactor::{Core, Handle};
use tokio_timer::Timer;

static CONFIG_PATH_ARG: &'static str = "PATH";

/// Runs linkerd-tcp.
///
/// Accepts a configuration file
fn main() {
    // Configure the logger from the RUST_LOG environment variable.
    drop(pretty_env_logger::init());

    // Load command-line options.
    let opts = ClapApp::new(crate_name!())
        .version(crate_version!())
        .about(crate_description!())
        .arg(
            Arg::with_name(CONFIG_PATH_ARG)
                .required(true)
                .index(1)
                .help("Config file path."),
        )
        .get_matches();

    // Parse configuration file.
    let config: AppConfig = {
        let path = opts.value_of(CONFIG_PATH_ARG).unwrap();
        let mut txt = String::new();
        let res = match path {
            "-" => ::std::io::stdin().read_to_string(&mut txt),
            path => fs::File::open(path).and_then(|mut f| f.read_to_string(&mut txt)),
        };
        match res {
            Err(e) => panic!("error reading configuration from {}: {}", path, e),
            Ok(_) => txt.parse().expect("failed to parse configuration"),
        }
    };
    debug!("parsed config: {:?}", config);

    // Process the configuration, splitting it into two threads. These threads are
    // connected by synchronization primitives as needed, but no work is being done yet.
    // Next, we'll attach each of these to a reactor in an independent thread, driving
    // both admin and serving work.
    let App { routers, admin } = config.into_app().expect("failed to load configuration");
    debug!("loaded app");

    let (closer, closed) = app::closer();

    // A single timer for the whole process.  The default hashwheel timer has a
    // granularity of 100ms.
    let timer = Timer::default();

    // Create a background admin thread that runs an admin server and executes executes
    // namerd resolutions
    let admin_thread = spawn_admin(admin, closer, &timer);
    run_routers(routers, closed, &timer);
    admin_thread.join().expect("failed to join admin thread");
    debug!("stopped")
}

fn spawn_admin(admin: AdminRunner, closer: app::Closer, timer: &Timer) -> thread::JoinHandle<()> {
    let timer = timer.clone();
    thread::Builder::new()
        .name("admin".into())
        .spawn(move || {
            debug!("running admin server");
            let mut core = Core::new().expect("failed to initialize admin reactor");
            admin.run(closer, &mut core, &timer).expect(
                "failed to run the admin server",
            );
        })
        .expect("failed to spawn admin thread")
}

fn run_routers(routers: VecDeque<RouterSpawner>, closed: app::Closed, timer: &Timer) {
    // Schedule all routers on the main thread.
    let mut core = Core::new().expect("failed to initialize server reactor");
    spawn_routers(routers, &core.handle(), timer);

    // Run until the admin thread closes the application.
    debug!("running until admin server closes");
    core.run(closed).expect("failed to run");
}

fn spawn_routers(mut routers: VecDeque<RouterSpawner>, reactor: &Handle, timer: &Timer) {
    while let Some(r) = routers.pop_front() {
        debug!("spawning router");
        r.spawn(reactor, timer).expect("failed to spawn router");
    }
}
