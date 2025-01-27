use actor::{master::MasterActor, replica::ReplicaActor};
use config::{parse_config, Config, DBFile, ReplicationRole};
use connection::{stream::RedisStream, Connection};
use store::Store;

use std::net::{TcpListener, TcpStream};

pub mod actor;
pub mod config;
pub mod connection;
pub mod store;

fn main() {
    println!("Logs from your program will appear here!");
    let config = parse_config();

    if let ReplicationRole::Replica(_) = &config.replication.role {
        build_and_run_master();
    } else {
        build_and_run_replica();
    }
}

pub fn build_and_run_replica() {
    let config = parse_config();
    let store = build_store(&config);

    let listener = TcpListener::bind(format!("127.0.0.1:{}", config.port)).unwrap();
    listener
        .set_nonblocking(true)
        .expect("Cannot put TCP listener in non-blocking mode");

    let mut store = MasterActor::new(store, config.clone());

    let mut connections: Vec<Connection> = Vec::new();

    loop {
        if let Some(stream) = check_for_new_connections(&listener) {
            let conn = Connection::new(stream, store.get_tx());
            connections.push(conn);
        }

        for conn in connections.iter_mut() {
            conn.poll();
            store.poll();
        }
    }
}

pub fn build_and_run_master() {
    let config = parse_config();
    let store = build_store(&config);

    let listener = TcpListener::bind(format!("127.0.0.1:{}", config.port)).unwrap();
    listener
        .set_nonblocking(true)
        .expect("Cannot put TCP listener in non-blocking mode");

    let mut store = ReplicaActor::new(store, config.clone());
    let Some(mut connection_with_master) = store.init_replication() else {
        return;
    };
    connection_with_master.poll();
    store.poll();

    let mut connections: Vec<Connection> = Vec::new();
    connections.push(connection_with_master);

    loop {
        if let Some(stream) = check_for_new_connections(&listener) {
            let conn = Connection::new(stream, store.get_tx());
            connections.push(conn);
        }

        for conn in connections.iter_mut() {
            conn.poll();
            store.poll();
        }
    }
}

fn check_for_new_connections(listener: &TcpListener) -> Option<RedisStream<TcpStream>> {
    if let Ok((stream, _)) = listener.accept() {
        stream
            .set_nonblocking(true)
            .expect("Cannot put TCP stream in non-blocking mode");
        println!("New client connection");
        return Some(RedisStream::new(stream));
    }
    None
}

fn build_store(config: &Config) -> Store {
    if let Some(DBFile { dir, dbfilename }) = &config.dbfile {
        if let Some(store) = Store::from_dbfile(dir, dbfilename) {
            return store;
        }
    }
    Store::new()
}
