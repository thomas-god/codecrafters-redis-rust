#![allow(unused_imports)]
use config::{parse_config, Config, DBFile, ReplicationRole};
use fmt::format_array;
use parser::{parse_command, parse_simple_type};
use store::Store;
use task::RedisTask;

use std::{
    cell::Cell,
    collections::HashMap,
    env,
    io::{ErrorKind, Read, Write},
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

        send_command(&mut master_link, vec![String::from("PING")]);
        send_command(&mut master_link, vec![
            String::from("REPLCONF"),
            String::from("listening-port"),
            format!("{}", config.port),
        ]);
        send_command(&mut master_link, vec![
            String::from("REPLCONF"),
            String::from("capa"),
            String::from("psync2"),
        ]);
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

fn send_command(stream: &mut TcpStream, command: Vec<String>) -> Option<String> {
    let mut buffer = [0u8; 256];
    let message = format_array(command);
    stream.write_all(message.as_bytes()).unwrap();
    match stream.read(&mut buffer) {
        Ok(bytes_received) => {
            if let Some(parser::RESPSimpleType::String(content)) =
                parse_simple_type(&buffer[0..bytes_received])
            {
                println!("Received {} from master", content);
                return Some(content.to_string());
            } else {
                return None;
                // panic!("Master not responding");
            };
        }
        Err(_) => None
    }
}