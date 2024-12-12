use std::{cell::Cell, io::ErrorKind, net::TcpListener};

use crate::{config::Config, connections::client::ClientConnection, store::Store};

pub struct ReplicaInstance {
    listener: TcpListener,
    master_connection: ClientConnection,
    client_connections: Vec<ClientConnection>,
    store: Cell<Store>,
    config: Config,
}

impl ReplicaInstance {
    pub fn new(
        listener: TcpListener,
        master_connection: ClientConnection,
        client_connections: Vec<ClientConnection>,
        store: Cell<Store>,
        config: Config,
    ) -> ReplicaInstance {
        ReplicaInstance {
            listener,
            master_connection,
            client_connections,
            store,
            config,
        }
    }

    pub fn run(&mut self) -> ! {
        loop {
            self.check_for_new_connections();
            self.poll_master_connection();
            self.poll_client_connections();
            self.remove_inactive_connections();
        }
    }

    fn check_for_new_connections(&mut self) {
        match self.listener.accept() {
            Ok((stream, _)) => {
                let connection = ClientConnection::new(stream);
                self.client_connections.push(connection);
            }
            Err(ref e) if e.kind() == ErrorKind::WouldBlock => {
                // println!("No new connection");
            }
            Err(err) => {
                println!("An error occured: {}", err);
            }
        }
    }

    fn poll_master_connection(&mut self) {
        if self.master_connection.active {
            self.master_connection.poll(&mut self.store, &self.config);
        }
    }

    fn poll_client_connections(&mut self) {
        for client in self.client_connections.iter_mut() {
            (*client).poll(&mut self.store, &self.config);
        }
    }

    fn remove_inactive_connections(&mut self) {
        self.client_connections.retain(|task| task.active);
    }
}
