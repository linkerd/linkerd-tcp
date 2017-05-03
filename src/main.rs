//#[macro_use]
extern crate clap;
//#[macro_use]
extern crate log;
extern crate pretty_env_logger;
extern crate linkerd_tcp;

// use clap::{Arg, App};
// use linkerd_tcp::app::{self, Runner};
// use std::fs;
// use std::io::Read;
// use std::thread;

// static CONFIG_PATH_ARG: &'static str = "PATH";

fn main() {
    // Configure the logger from the RUST_LOG environment variable.
    drop(pretty_env_logger::init());

    //     // Load command-line options.
    //     let opts = App::new(crate_name!())
    //         .version(crate_version!())
    //         .about(crate_description!())
    //         .arg(Arg::with_name(CONFIG_PATH_ARG)
    //             .required(true)
    //             .index(1)
    //             .help("Config file path."))
    //         .get_matches();

    //     // Parse configuration file.
    //     let config = {
    //         let path = opts.value_of(CONFIG_PATH_ARG).unwrap();
    //         let mut txt = String::new();
    //         fs::File::open(path)
    //             .and_then(|mut f| f.read_to_string(&mut txt))
    //             .expect("failed to read config");
    //         app::config::from_str(&txt).expect("configuration error")
    //     };

    //     // Process the configuration, splitting it into two threads. These threads are
    //     // connected by synchronization primitives as needed, but no work is being done yet.
    //     // Next, we'll attach each of these to a reactor in an independent thread, driving
    //     // both admin and serving work.
    //     let (admin, proxies) = app::configure(config);

    //     // Run admin in a new thread.z
    //     let admin_thread = thread::Builder::new()
    //         .name("admin".into())
    //         .spawn(move || admin.run().expect("could not run admin"))
    //         .expect("could not spawn admin thread");

    //     proxies.run().expect("could not run proxies");
    //     admin_thread.join().expect("admin thread failed to join");
    //     info!("Closing.")
}
