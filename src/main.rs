// #![allow(unused_imports)]
use config::{parse_config, Config, DBFile, ReplicationRole};
use connections::client::ConnectionRole;
use connections::{client::ClientConnection, PollResult};
use store::Store;

use std::{
    cell::Cell,
    io::ErrorKind,
    net::{TcpListener, TcpStream},
};

pub mod config;
pub mod connections;
pub mod fmt;
pub mod store;

fn main() {
    println!("Logs from your program will appear here!");
    let config = parse_config();
    let mut client_connections: Vec<ClientConnection> = Vec::new();
    let mut store = Cell::new(build_store(&config));

    let listener = TcpListener::bind(format!("127.0.0.1:{}", config.port)).unwrap();
    listener
        .set_nonblocking(true)
        .expect("Cannot put TCP listener in non-blocking mode");

    if let ReplicationRole::Replica((host, port)) = &config.replication.role {
        let Some(master_link) = TcpStream::connect(format!("{host}:{port}")).ok() else {
            panic!("Could not connect to master instance.");
        };
        let mut connection = ClientConnection::new(master_link);
        if connection.replication_handshake(&config, &mut store).is_none() {
            println!("Error when doing the replication handshake");
        }
        client_connections.push(connection);
        println!("Replication handshake done");
    }

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
                replica.send_string(cmd);
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
