#![allow(unused_imports)]
use store::Store;
use task::RedisTask;

use std::{cell::Cell, collections::HashMap, env, io::ErrorKind, net::TcpListener};

pub mod parser;
pub mod store;
pub mod task;

fn main() {
    println!("Logs from your program will appear here!");
    let config = parse_args();

    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    listener
        .set_nonblocking(true)
        .expect("Cannot put TCP listener in non-blocking mode");

    let mut tasks: Vec<RedisTask> = Vec::new();
    let mut store = Cell::new(Store::new());

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

fn parse_args() -> HashMap<String, String> {
    let mut args_iter = env::args();
    let mut args: HashMap<String, String> = HashMap::new();

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
