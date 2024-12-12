// #![allow(unused_imports)]
use config::{parse_config, Config, DBFile, ReplicationRole};
use connections::client::ClientConnection;
use instance::{master::MasterInstance, replica::ReplicaInstance};
use store::Store;

use std::{
    cell::Cell,
    net::{TcpListener, TcpStream},
};

pub mod config;
pub mod connections;
pub mod instance;
pub mod store;

fn main() {
    println!("Logs from your program will appear here!");
    let config = parse_config();
    let client_connections: Vec<ClientConnection> = Vec::new();
    let mut store = Cell::new(build_store(&config));

    let listener = TcpListener::bind(format!("127.0.0.1:{}", config.port)).unwrap();
    listener
        .set_nonblocking(true)
        .expect("Cannot put TCP listener in non-blocking mode");

    if let ReplicationRole::Replica((host, port)) = &config.replication.role {
        let Some(master_link) = TcpStream::connect(format!("{host}:{port}")).ok() else {
            panic!("Could not connect to master instance.");
        };
        let mut master_connection = ClientConnection::new(master_link);
        if master_connection
            .replication_handshake(&config, &mut store)
            .is_none()
        {
            println!("Error when doing the replication handshake");
        }
        println!("Replication handshake done");

        let mut instance = ReplicaInstance::new(
            listener,
            master_connection,
            client_connections,
            store,
            config,
        );
        instance.run()
    } else {
        let mut instance = MasterInstance::new(listener, client_connections, store, config);
        instance.run()
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
