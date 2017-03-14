#[macro_use]
extern crate clap;
#[macro_use]
extern crate log;
extern crate env_logger;
extern crate linkerd_tcp;
extern crate tokio_core;

use clap::{Arg, App};
use linkerd_tcp::app;
use std::fs;
use std::io::Read;
use tokio_core::reactor::Core;

fn main() {
    // Configure the logger from the RUST_LOG environment variable.
    drop(env_logger::init());

    // Parse and load command-line options.
    let opts = mk_app().get_matches();
    let config_path = opts.value_of(CONFIG_PATH_ARG).unwrap();
    let config_str: String = {
        let mut s = String::new();
        fs::File::open(config_path)
            .unwrap()
            .read_to_string(&mut s)
            .unwrap();
        s
    };

    let mut core = Core::new().unwrap();
    let running = app::run(&config_str, &core.handle()).unwrap();
    core.run(running).unwrap();
    info!("Closing.")
}

static CONFIG_PATH_ARG: &'static str = "PATH";

fn mk_app() -> App<'static, 'static> {
    App::new(crate_name!())
        .version(crate_version!())
        .about(crate_description!())
        .arg(Arg::with_name(CONFIG_PATH_ARG)
            .required(true)
            .index(1)
            .help("Config file path."))
}