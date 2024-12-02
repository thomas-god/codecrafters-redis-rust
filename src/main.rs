#![allow(unused_imports)]
use config::{parse_config, Config, DBFile, ReplicationRole};
use store::Store;
use task::RedisTask;

use std::{
    cell::Cell,
    collections::HashMap,
    env,
    io::{ErrorKind, Write},
    net::{TcpListener, TcpStream},
};

pub mod config;
pub mod fmt;
pub mod parser;
pub mod store;
pub mod task;

fn main() {
    println!("Logs from your program will appear here!");
    let config = parse_config();

    if let ReplicationRole::Replica((host, port)) = &config.replication.role {
        let mut master_link = TcpStream::connect(format!("{host}:{port}")).unwrap();
        master_link
            .write_all(String::from("*1\r\n$4\r\nPING\r\n").as_bytes())
            .unwrap();
    };

    let listener = TcpListener::bind(format!("127.0.0.1:{}", config.port)).unwrap();
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

fn build_store(config: &Config) -> Store {
    if let Some(DBFile { dir, dbfilename }) = &config.dbfile {
        if let Some(store) = Store::from_dbfile(dir, dbfilename) {
            return store;
        }
    }
    Store::new()
}
