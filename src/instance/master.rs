use std::{
    cell::Cell,
    io::ErrorKind,
    net::TcpListener,
    time::{Duration, Instant},
};

use crate::{
    config::Config,
    connections::{
        client::{ClientConnection, ConnectionRole},
        fmt::format_array,
        PollResult, ReplicationCheckRequest,
    },
    store::Store,
};

pub struct MasterInstance {
    listener: TcpListener,
    client_connections: Vec<ClientConnection>,
    store: Cell<Store>,
    config: Config,
    replication_offset: usize,
    last_acked_offset: usize,
}

impl MasterInstance {
    pub fn new(
        listener: TcpListener,
        client_connections: Vec<ClientConnection>,
        store: Cell<Store>,
        config: Config,
    ) -> MasterInstance {
        MasterInstance {
            listener,
            client_connections,
            store,
            config,
            replication_offset: 0,
            last_acked_offset: 0,
        }
    }

    pub fn run(&mut self) -> ! {
        loop {
            self.check_for_new_connections();
            let (writes_to_replicate, replication_request) = self.poll_connections();
            self.propagate_write_commands(writes_to_replicate);
            self.check_replicas_offset(replication_request);
            self.update_replica_count();
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

    fn poll_connections(&mut self) -> (Vec<String>, Option<ReplicationCheckRequest>) {
        let mut writes_to_replicate: Vec<String> = Vec::new();
        let mut wait_for_acks: Option<ReplicationCheckRequest> = None;
        for client in self.client_connections.iter_mut() {
            let results = (*client).poll(&mut self.store, &self.config);
            for res in results {
                if let PollResult::Write(cmd) = &res {
                    writes_to_replicate.push(cmd.clone());
                }
                if let PollResult::WaitForAcks(request) = &res {
                    wait_for_acks = Some(*request);
                }
            }
        }
        (writes_to_replicate, wait_for_acks)
    }

    fn propagate_write_commands(&mut self, writes_to_replicate: Vec<String>) {
        for cmd in writes_to_replicate.iter() {
            self.replication_offset += cmd.as_bytes().len();
            for replica in self
                .client_connections
                .iter_mut()
                .filter(|c| c.connected_with == ConnectionRole::Replica)
            {
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

    fn check_replicas_offset(&mut self, check_request: Option<ReplicationCheckRequest>) {
        let Some(check_request) = check_request else {
            return;
        };
        println!(
            "Expecting at least {} replicas to ack within {}ms",
            check_request.number_of_replicas,
            check_request.timeout.unwrap_or(0)
        );
        let mut n_replicas_checked = 0;

        if self.last_acked_offset == self.replication_offset {
            n_replicas_checked = self
                .client_connections
                .iter_mut()
                .filter(|c| c.connected_with == ConnectionRole::Replica)
                .count();
        } else {
            for replica in self
                .client_connections
                .iter_mut()
                .filter(|c| c.connected_with == ConnectionRole::Replica)
            {
                println!("Asking for replication ack");
                replica.send_string(&format_array(&vec![
                    String::from("REPLCONF"),
                    String::from("GETACK"),
                    String::from("*"),
                ]));
            }
        }

        let delay = Duration::from_millis(check_request.timeout.unwrap_or(0) as u64);
        let start = Instant::now();
        while n_replicas_checked < check_request.number_of_replicas {
            if start.elapsed() > delay {
                println!("Timeout reached for acks");
                break;
            }
            for replica in self
                .client_connections
                .iter_mut()
                .filter(|c| c.connected_with == ConnectionRole::Replica)
            {
                let results = replica.poll(&mut self.store, &self.config);
                if results
                    .iter()
                    .any(|res| matches!(res, PollResult::AckSuccessful))
                {
                    n_replicas_checked += 1;
                    println!("Replicas count increased to {n_replicas_checked}");
                }
            }
        }

        println!(
            "Iteration done after {:?} and {} replicas acks",
            start.elapsed(),
            n_replicas_checked
        );

        for client in self
            .client_connections
            .iter_mut()
            .filter(|c| c.connected_with == ConnectionRole::Client)
        {
            client.notify_replication_ack(n_replicas_checked);
        }
    }
}
