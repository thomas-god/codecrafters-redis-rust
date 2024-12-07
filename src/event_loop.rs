use std::{cell::Cell, io::ErrorKind, net::TcpListener};

use crate::{
    config::Config,
    connections::{
        client::{ClientConnection, ConnectionRole},
        PollResult,
    },
    store::Store,
};

pub struct EventLoop {
    listener: TcpListener,
    client_connections: Vec<ClientConnection>,
    store: Cell<Store>,
    config: Config,
}

impl EventLoop {
    pub fn new(
        listener: TcpListener,
        client_connections: Vec<ClientConnection>,
        store: Cell<Store>,
        config: Config,
    ) -> EventLoop {
        EventLoop {
            listener,
            client_connections,
            store,
            config,
        }
    }

    pub fn run(&mut self) -> ! {
        loop {
            // Check for new client connection to handle
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

            // Poll existing connections for pending command
            let writes_to_replicate = self.poll_connections();

            self.propagate_write_commands(writes_to_replicate);
            self.update_replica_count();
            self.remove_inactive_connections();
        }
    }

fn poll_connections(&mut self) -> Vec<String> {
        let mut writes_to_replicate: Vec<String> = Vec::new();
        for client in self.client_connections.iter_mut() {
            let results = (*client).poll(&mut self.store, &self.config);
            for res in results {
                if let PollResult::Write(cmd) = res {
                    writes_to_replicate.push(cmd.clone());
                }
            }
        }
        writes_to_replicate
    }

fn propagate_write_commands(&mut self, writes_to_replicate: Vec<String>) {
        for replica in self
            .client_connections
            .iter_mut()
            .filter(|c| c.connected_with == ConnectionRole::Replica)
        {
            for cmd in writes_to_replicate.iter() {
                replica.send_string(cmd);
            }
        }
    }

fn remove_inactive_connections(&mut self) {
        self.client_connections.retain(|task| task.active);
    }

fn update_replica_count(&mut self) {
        let n_replicas = self
            .client_connections
            .iter()
            .filter(|c| c.connected_with == ConnectionRole::Replica)
            .count();
        self.store.get_mut().n_replicas = n_replicas as u64;
    }
}
