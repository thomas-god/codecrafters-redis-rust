#![allow(unused_imports)]
use bytes::buf;
use config::{parse_config, Config, DBFile, ReplicationRole};
use connections::client::ConnectionRole;
use connections::{client::ClientConnection, PollResult};
use fmt::format_array;
use parser::{parse_command, parse_simple_type};
use store::Store;

use std::cell::RefCell;
use std::str::from_utf8;
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

    // let mut master_link : Option<TcpStream> = None;
    // if let ReplicationRole::Replica((host, port)) = &config.replication.role {
    //     println!("Starting replication handshake");
    //     let mut master_link = TcpStream::connect(format!("{host}:{port}")).unwrap();
    //     let mut buffer = [0u8; 2048];

    //     send_command(&mut master_link, &mut buffer, vec![String::from("PING")]);
    //     send_command(
    //         &mut master_link,
    //         &mut buffer,
    //         vec![
    //             String::from("REPLCONF"),
    //             String::from("listening-port"),
    //             format!("{}", config.port),
    //         ],
    //     );
    //     send_command(
    //         &mut master_link,
    //         &mut buffer,
    //         vec![
    //             String::from("REPLCONF"),
    //             String::from("capa"),
    //             String::from("psync2"),
    //         ],
    //     );
    //     send_command(
    //         &mut master_link,
    //         &mut buffer,
    //         vec![String::from("PSYNC"), String::from("?"), String::from("-1")],
    //     );
    //     let n = master_link.read(&mut buffer).ok().unwrap();
    //     println!("Received {n} bytes");
    //     let rdb_content = from_utf8(&buffer[0..n]).unwrap();
    //     println!("Received RDB file: {}", rdb_content);
    // };

    let listener = TcpListener::bind(format!("127.0.0.1:{}", config.port)).unwrap();
    listener
        .set_nonblocking(true)
        .expect("Cannot put TCP listener in non-blocking mode");

    let master_connection = ClientConnection::from_handshake(&config);
    let mut client_connections: Vec<ClientConnection> = Vec::new();
    if let Some(master) = master_connection {
        println!("Adding master to connections");
        client_connections.push(master);
    }
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

        // Poll existing connections for pending command
        let mut writes_to_replicate: Vec<String> = Vec::new();
        for client in client_connections.iter_mut() {
            let results = (*client).poll(&mut store, &config);
            for res in results {
                if let PollResult::Write(cmd) = res {
                    writes_to_replicate.push(cmd.clone());
                }
            }
        }

        // Propagate writes to replica connections
        for replica in client_connections
            .iter_mut()
            .filter(|c| c.connected_with == ConnectionRole::Replica)
        {
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
