// #![allow(unused_imports)]
use config::{parse_config, Config, DBFile, ReplicationRole};
use master::instance::MasterInstance;
use replica::instance::ReplicaInstance;
use store::Store;

use std::{cell::Cell, net::TcpListener};

pub mod config;
pub mod connections;
pub mod master;
pub mod replica;
pub mod store;

fn main() {
    println!("Logs from your program will appear here!");
    let config = parse_config();
    let store = Cell::new(build_store(&config));

    let listener = TcpListener::bind(format!("127.0.0.1:{}", config.port)).unwrap();
    listener
        .set_nonblocking(true)
        .expect("Cannot put TCP listener in non-blocking mode");

    if let ReplicationRole::Replica(_) = &config.replication.role {
        let Some(mut instance) = ReplicaInstance::new(listener, store, config) else {
            panic!("Unable to start connection with master instance")
        };
        instance.run()
    } else {
        let mut instance = MasterInstance::new(listener, store, config);
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
