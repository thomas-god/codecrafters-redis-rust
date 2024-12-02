#![allow(unused_imports)]
use store::Store;
use task::RedisTask;

use std::{cell::Cell, collections::HashMap, env, io::ErrorKind, net::TcpListener};

pub mod fmt;
pub mod parser;
pub mod store;
pub mod task;

type Config = HashMap<String, String>;

fn main() {
    println!("Logs from your program will appear here!");
    let config = parse_args();

    let default_port = String::from("6379");
    let port = config.get("port").unwrap_or(&default_port);

    let listener = TcpListener::bind(format!("127.0.0.1:{port}")).unwrap();
    listener
        .set_nonblocking(true)
        .expect("Cannot put TCP listener in non-blocking mode");

    let mut tasks: Vec<RedisTask> = Vec::new();
    let mut store = Cell::new(build_store(&config));

    loop {
        match listener.accept() {
            Ok((stream, _)) => {
                let task = RedisTask::new(stream);
                tasks.push(task);
            }
            Err(ref e) if e.kind() == ErrorKind::WouldBlock => {
                // println!("No new connection");
            }
            Err(err) => {
                println!("An error occured: {}", err);
            }
        }

        for task in tasks.iter_mut() {
            (*task).poll(&mut store, &config);
        }

        tasks.retain(|task| task.active);
    }
}

fn parse_args() -> Config {
    let mut args_iter = env::args();
    let mut args: Config = HashMap::new();

    // Drop first args, see `env::args()`
    let _ = args_iter.next();

    while let (Some(cmd), Some(param)) = (args_iter.next(), args_iter.next()) {
        let (prefix, cmd) = cmd.split_at(2);
        if prefix == "--" {
            args.insert(cmd.to_string(), param);
        }
    }

    args
}

fn build_store(config: &Config) -> Store {
    if let (Some(dir), Some(dbfilename)) = (config.get("dir"), config.get("dbfilename")) {
        if let Some(store) = Store::from_dbfile(dir, dbfilename) {
            return store;
        }
    }
    Store::new()
}
