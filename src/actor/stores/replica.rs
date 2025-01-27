use std::{
    net::TcpStream,
    sync::mpsc::{channel, Receiver, Sender},
};

use crate::{
    actor::{connection::Connection, ConnectionMessage, StoreMessage},
    config::{Config, ReplicationRole},
    connections::{
        fmt::{format_array, format_string},
        parser::{BufferType, Command, CommandVerb},
        stream::RedisStream,
    },
    store::Store,
};

pub struct ReplicaActor {
    store: Store,
    config: Config,
    tx_clients: Sender<StoreMessage>,
    rx_clients: Receiver<StoreMessage>,
    tx_master: Sender<StoreMessage>,
    replication_offset: usize,
    rx_master: Receiver<StoreMessage>,
}

impl ReplicaActor {
    pub fn new(store: Store, config: Config) -> ReplicaActor {
        let (tx_clients, rx_clients) = channel();
        let (tx_master, rx_master) = channel();

        ReplicaActor {
            store,
            config,
            tx_clients,
            rx_clients,
            rx_master,
            tx_master,
            replication_offset: 0,
        }
    }

    pub fn poll(&mut self) {
        while let Ok(message) = self.rx_master.try_recv() {
            if let StoreMessage::NewBuffer {
                value: BufferType::Command(cmd),
                tx_back,
            } = message
            {
                println!("{cmd:?}");
                self.process_command(&cmd, tx_back);
                self.track_replication_offset(cmd.cmd);
            }
        }
        while let Ok(message) = self.rx_clients.try_recv() {
            match message {
                StoreMessage::NewBuffer {
                    value: BufferType::Command(cmd),
                    tx_back,
                } => {
                    println!("{cmd:?}");
                    self.process_command(&cmd, tx_back);
                }
                _ => todo!(),
            }
        }
    }

    pub fn get_tx(&self) -> Sender<StoreMessage> {
        self.tx_clients.clone()
    }

    pub fn init_replication(&mut self) -> Option<Connection> {
        let ReplicationRole::Replica((host, port)) = &self.config.replication.role else {
            return None;
        };
        let Some(master_stream) = TcpStream::connect(format!("{host}:{port}")).ok() else {
            panic!("Could not connect to master instance.");
        };
        let mut master_stream = RedisStream::new(master_stream);

        println!("Starting replication handshake with {host}:{port}");
        println!("Enabling blocking behavior of the TCP stream");
        master_stream.set_stream_nonblocking_behavior(false);
        // let master_conn = Connection::new(master_stream, self.tx_master);

        println!("Replication: sending PING");
        master_stream.send_string(&format_array(&vec![String::from("PING")]));
        let res = master_stream.read();
        println!("{res:?}");

        println!("Replication: sending REPLCONF (1/2)");
        master_stream.send_string(&format_array(&vec![
            String::from("REPLCONF"),
            String::from("listening-port"),
            format!("{}", self.config.port),
        ]));
        let res = master_stream.read();
        println!("{res:?}");

        println!("Replication: sending REPLCONF (2/2)");
        master_stream.send_string(&format_array(&vec![
            String::from("REPLCONF"),
            String::from("capa"),
            String::from("psync2"),
        ]));
        let res = master_stream.read();
        println!("{res:?}");

        println!("Replication: sending PSYNC");
        master_stream.send_string(&format_array(&vec![
            String::from("PSYNC"),
            String::from("?"),
            String::from("-1"),
        ]));
        println!("{res:?}");

        println!("Handshake done");

        master_stream.set_stream_nonblocking_behavior(true);
        Some(Connection::new(master_stream, self.tx_master.clone()))
    }

    fn track_replication_offset(&mut self, cmd: Vec<String>) {
        let n_bytes = format_array(&cmd).len();
        match cmd.first() {
            Some(cmd) if cmd == "PING" || cmd == "SET" || cmd == "REPLCONF" => {
                self.replication_offset += n_bytes;
                println!("New replication offset: {}", self.replication_offset);
            }
            _ => {}
        }
    }

    fn process_command(&mut self, cmd: &Command, tx_back: Sender<ConnectionMessage>) {
        println!("Processing command: {cmd:?}");
        let Command { verb, cmd } = cmd;
        match verb {
            CommandVerb::ECHO => self.process_echo(cmd, tx_back),
            CommandVerb::SET => self.process_set(cmd, tx_back),
            CommandVerb::GET => self.process_get(cmd, tx_back),
            CommandVerb::CONFIG => self.process_config(cmd, tx_back),
            CommandVerb::KEYS => self.process_keys(tx_back),
            CommandVerb::INFO => self.process_info(cmd, tx_back),
            CommandVerb::REPLCONF => self.process_replconf(cmd, tx_back),
            unsupported_verb => self.log_unsupported_verb(unsupported_verb),
        };
    }

    fn process_echo(&mut self, command: &[String], tx_back: Sender<ConnectionMessage>) {
        if let Some(message) = command.get(1) {
            let message = format!("${}\r\n{}\r\n", message.len(), message);
            tx_back
                .send(ConnectionMessage::SendString(message))
                .unwrap();
        }
    }

    fn process_set(&mut self, command: &[String], _tx_back: Sender<ConnectionMessage>) {
        let (Some(key), Some(value)) = (command.get(1), command.get(2)) else {
            return;
        };

        let option = command.get(3);
        let option_value: Option<usize> = match command.get(4) {
            Some(option_value) => option_value.parse::<usize>().ok(),
            _ => None,
        };
        let ttl = match (option, option_value) {
            (Some(cmd), Some(cmd_value)) if cmd == "px" => Some(cmd_value),
            _ => None,
        };

        println!("Setting {}: {}", key, value);
        self.store.set_string(key, value, ttl);
    }

    fn process_get(&mut self, command: &[String], tx_back: Sender<ConnectionMessage>) {
        let Some(key) = command.get(1) else {
            return;
        };
        let value = self.store.get_string(key);
        let message = ConnectionMessage::SendString(format_string(value));
        tx_back.send(message).unwrap();
    }

    fn process_config(&mut self, command: &[String], tx_back: Sender<ConnectionMessage>) {
        let (Some(action), Some(key)) = (command.get(1), command.get(2)) else {
            return;
        };
        if *action == "GET" {
            let Some(value) = self.config.get_arg(key) else {
                return;
            };
            let message = format!(
                "*2\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                key.len(),
                key,
                value.len(),
                value
            );
            tx_back
                .send(ConnectionMessage::SendString(message))
                .unwrap();
        }
    }

    fn process_keys(&mut self, tx_back: Sender<ConnectionMessage>) {
        let mut response = String::new();
        let keys = self.store.get_keys();
        response.push_str(&format!("*{}\r\n", keys.len()));
        for key in keys {
            response.push_str(&format!("${}\r\n{}\r\n", key.len(), key));
        }
        tx_back
            .send(ConnectionMessage::SendString(response))
            .unwrap();
    }

    fn process_info(&mut self, command: &[String], tx_back: Sender<ConnectionMessage>) {
        match command.get(1) {
            Some(section) if *section == "replication" => {
                let mut response = String::new();
                let role = match self.config.replication.role {
                    ReplicationRole::Master => String::from("master"),
                    ReplicationRole::Replica(_) => String::from("slave"),
                };
                response.push_str(&format!("role:{role}\r\n"));
                response.push_str(&format!(
                    "master_replid:{}\r\n",
                    self.config.replication.replid
                ));
                response.push_str(&format!(
                    "master_repl_offset:{}\r\n",
                    self.config.replication.repl_offset
                ));
                tx_back
                    .send(ConnectionMessage::SendString(format_string(Some(response))))
                    .unwrap();
            }
            _ => panic!(),
        }
    }

    fn process_replconf(&mut self, command: &[String], tx_back: Sender<ConnectionMessage>) {
        match command.get(1) {
            Some(option) if option == "GETACK" => {
                let message = format_array(&vec![
                    String::from("REPLCONF"),
                    String::from("ACK"),
                    format!("{}", self.replication_offset),
                ]);
                tx_back
                    .send(ConnectionMessage::SendString(message))
                    .unwrap();
            }
            _ => {
                tx_back
                    .send(ConnectionMessage::SendString(String::from("+OK\r\n")))
                    .unwrap();
            }
        };
    }

    fn log_unsupported_verb(&self, verb: &CommandVerb) {
        println!("{verb:?} not implemented for Replica to Master connection");
    }
}
