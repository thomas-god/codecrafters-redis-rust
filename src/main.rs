#![allow(unused_imports)]
use bytes::buf;
use config::{parse_config, Config, DBFile, ReplicationRole};
use connections::{client::ClientConnection, PollResult};
use fmt::format_array;
use parser::{parse_command, parse_simple_type};
use store::Store;

use std::cell::RefCell;
use std::{
    cell::Cell,
    collections::HashMap,
    env,
    io::{ErrorKind, Read, Write},
    net::{TcpListener, TcpStream},
};

pub mod config;
pub mod connections;
pub mod fmt;
pub mod parser;
pub mod store;

fn main() {
    println!("Logs from your program will appear here!");
    let config = parse_config();

    if let ReplicationRole::Replica((host, port)) = &config.replication.role {
        println!("Starting replication handshake");
        let mut master_link = TcpStream::connect(format!("{host}:{port}")).unwrap();
        let mut buffer = [0u8; 2048];

        send_command(&mut master_link, &mut buffer, vec![String::from("PING")]);
        send_command(
            &mut master_link,
            &mut buffer,
            vec![
                String::from("REPLCONF"),
                String::from("listening-port"),
                format!("{}", config.port),
            ],
        );
        send_command(
            &mut master_link,
            &mut buffer,
            vec![
                String::from("REPLCONF"),
                String::from("capa"),
                String::from("psync2"),
            ],
        );

        if let Some(bytes_received) = send_command(
            &mut master_link,
            &mut buffer,
            vec![String::from("PSYNC"), String::from("?"), String::from("-1")],
        ) {
            let content = parse_simple_type(&buffer[0..bytes_received]);
            println!("{:?}", content.unwrap());
        }
    };

    let listener = TcpListener::bind(format!("127.0.0.1:{}", config.port)).unwrap();
    listener
        .set_nonblocking(true)
        .expect("Cannot put TCP listener in non-blocking mode");

    let mut client_connections: Vec<ClientConnection> = Vec::new();
    let mut store = Cell::new(build_store(&config));

    loop {
        // Check for new client connection to handle
        match listener.accept() {
            Ok((stream, _)) => {
                let connection = ClientConnection::new(stream);
                client_connections.push(connection);
            }
            Err(ref e) if e.kind() == ErrorKind::WouldBlock => {
                // println!("No new connection");
            }
            Err(err) => {
                println!("An error occured: {}", err);
            }
        }

        // Poll existing connections for pendign command
        let mut writes_to_replicate: Vec<String> = Vec::new();
        for client in client_connections.iter_mut() {
            match (*client).poll(&mut store, &config) {
                Some(PollResult::Write(cmd)) => {
                    writes_to_replicate.push(cmd.clone());
                }
                _ => {}
            };
        }

        // Propagate writes to replica connections
        for replica in client_connections.iter_mut().filter(|c| c.replica) {
            for cmd in writes_to_replicate.iter() {
                replica.send_command(&cmd);
            }
        }

        // Drop inactive connections
        client_connections.retain(|task| task.active);
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

fn send_command(stream: &mut TcpStream, buffer: &mut [u8], command: Vec<String>) -> Option<usize> {
    let message = format_array(command);
    stream.write_all(message.as_bytes()).unwrap();
    stream.read(buffer).ok()
}
