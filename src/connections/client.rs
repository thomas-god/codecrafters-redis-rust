use std::{cell::Cell, fs, net::TcpStream};

use crate::{
    config::{Config, ReplicationRole},
    connections::parser::{BufferType, Command},
    store::Store,
};

use super::{
    fmt::{format_array, format_string},
    stream::RedisStream,
    PollResult, ReplicationCheckRequest,
};

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum ConnectionRole {
    Master,
    Client,
    Replica,
}

pub struct ClientConnection {
    stream: RedisStream<TcpStream>,
    pub active: bool,
    pub connected_with: ConnectionRole,
    wait_for_replication_ack: bool,
    replication: Option<Replication>,
}

#[derive(Debug)]
struct Replication {
    replication_offset: usize,
    last_offset_checked: usize,
}

impl Replication {
    fn match_offsets(&mut self) {
        self.last_offset_checked = self.replication_offset;
    }

    fn need_to_ask_for_replication_ack(&self) -> bool {
        println!(
            "current offset: {}, last offset {}",
            self.replication_offset, self.last_offset_checked
        );
        self.last_offset_checked < self.replication_offset
    }
}

impl ClientConnection {
    pub fn new(stream: TcpStream) -> ClientConnection {
        stream
            .set_nonblocking(true)
            .expect("Cannot put TCP stream in non-blocking mode");
        let stream = RedisStream::new(stream);
        ClientConnection {
            stream,
            active: true,
            wait_for_replication_ack: false,
            connected_with: ConnectionRole::Client,
            replication: None,
        }
    }

    pub fn poll(&mut self, global_state: &mut Cell<Store>, config: &Config) -> Vec<PollResult> {
        match self.stream.read() {
            Some(elements) => {
                let mut poll_results: Vec<PollResult> = vec![];
                for element in elements {
                    match element {
                        BufferType::Command(Command { cmd, verb: _ }) => {
                            if let Some(result) = self.process_command(&cmd, global_state, config) {
                                poll_results.push(result);
                            }
                            self.track_replication_offset(cmd);
                        }
                        elem => println!("Nothing to do for: {elem:?}"),
                    }
                }
                if poll_results
                    .iter()
                    .any(|r| *r == PollResult::PromoteToReplica)
                {
                    println!(
                        "Promoting connection to replica role, will propagate future writes to it."
                    );
                    self.connected_with = ConnectionRole::Replica;
                }
                poll_results
            }
            _ => {
                println!("Unable to read from stream");
                self.active = false;
                vec![]
            }
        }
    }

    fn track_replication_offset(&mut self, cmd: Vec<String>) {
        let n_bytes = format_array(&cmd).len();
        if let Some(Replication {
            ref mut replication_offset,
            last_offset_checked: _,
        }) = self.replication
        {
            match cmd.first() {
                Some(cmd) if cmd == "PING" || cmd == "SET" || cmd == "REPLCONF" => {
                    *replication_offset += n_bytes;
                    println!("New replication offset: {replication_offset}");
                }
                _ => {}
            }
        };
    }

    pub fn send_command(&mut self, command: &Vec<String>) {
        let message = format_array(command);
        self.send_string(&message);
        self.stream.read();
    }

    pub fn send_string(&mut self, message: &str) {
        self.stream.send(message);
    }

    pub fn notify_replication_ack(&mut self, n_replicas: usize) {
        if !self.wait_for_replication_ack {
            return;
        }
        println!("Notifying client of replication ack");
        self.send_string(&format!(":{}\r\n", n_replicas));
        self.wait_for_replication_ack = false;
    }

    fn process_command(
        &mut self,
        cmd: &Vec<String>,
        global_state: &mut Cell<Store>,
        config: &Config,
    ) -> Option<PollResult> {
        println!("Processing command: {cmd:?}");
        if let Some(verb) = cmd.first() {
            match verb.as_str() {
                "PING" => self.process_ping(),
                "ECHO" => self.process_echo(cmd),
                "SET" => self.process_set(cmd, global_state),
                "GET" => self.process_get(cmd, global_state),
                "CONFIG" => self.process_config(cmd, config),
                "KEYS" => self.process_keys(cmd, global_state),
                "INFO" => self.process_info(cmd, config),
                "REPLCONF" => self.process_replconf(cmd),
                "PSYNC" => self.process_psync(config),
                "WAIT" => self.process_wait(cmd),
                v => {
                    println!("Found invalid verb to process: {v}");
                    None
                }
            }
        } else {
            None
        }
    }

    fn process_ping(&mut self) -> Option<PollResult> {
        if self.replication.is_some() {
            return None;
        };
        self.stream.send(&String::from("+PONG\r\n"));
        println!("Sending PONG back");
        None
    }

    fn process_echo(&mut self, command: &[String]) -> Option<PollResult> {
        if let Some(message) = command.get(1) {
            let message = format!("${}\r\n{}\r\n", message.len(), message);
            self.stream.send(&message);
            None
        } else {
            None
        }
    }

    fn process_set(
        &mut self,
        command: &[String],
        global_state: &mut Cell<Store>,
    ) -> Option<PollResult> {
        let (Some(key), Some(value)) = (command.get(1), command.get(2)) else {
            return None;
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
        global_state.get_mut().set(key, value, ttl);
        if let ConnectionRole::Client = &self.connected_with {
            if self.replication.is_none() {
                self.stream.send(&String::from("+OK\r\n"));
            }
        };
        Some(PollResult::Write(format_array(&command.to_vec())))
    }

    fn process_get(
        &mut self,
        command: &[String],
        global_state: &mut Cell<Store>,
    ) -> Option<PollResult> {
        let key = command.get(1)?;
        self.stream
            .send(&format_string(global_state.get_mut().get(key)));
        None
    }

    fn process_config(&mut self, command: &[String], config: &Config) -> Option<PollResult> {
        match command.get(1) {
            Some(action) if *action == "GET" => {
                let key = command.get(2)?;
                let value = config.get_arg(key)?.clone();
                let message = format!(
                    "*2\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                    key.len(),
                    key,
                    value.len(),
                    value
                );
                self.stream.send(&message);
                None
            }
            _ => panic!(),
        }
    }

    fn process_keys(
        &mut self,
        _command: &[String],
        global_state: &mut Cell<Store>,
    ) -> Option<PollResult> {
        let mut response = String::new();
        let keys = global_state.get_mut().get_keys();
        response.push_str(&format!("*{}\r\n", keys.len()));
        for key in keys {
            response.push_str(&format!("${}\r\n{}\r\n", key.len(), key));
        }
        self.stream.send(&response);
        None
    }

    fn process_info(&mut self, command: &[String], config: &Config) -> Option<PollResult> {
        match command.get(1) {
            Some(section) if *section == "replication" => {
                let mut response = String::new();
                let role = match config.replication.role {
                    ReplicationRole::Master => String::from("master"),
                    ReplicationRole::Replica(_) => String::from("slave"),
                };
                response.push_str(&format!("role:{role}\r\n"));
                response.push_str(&format!("master_replid:{}\r\n", config.replication.replid));
                response.push_str(&format!(
                    "master_repl_offset:{}\r\n",
                    config.replication.repl_offset
                ));
                self.stream.send(&format_string(Some(response)));
                None
            }
            _ => panic!(),
        }
    }

    fn process_replconf(&mut self, command: &[String]) -> Option<PollResult> {
        match command.get(1) {
            Some(option) if option == "GETACK" => {
                let message = format_array(&vec![
                    String::from("REPLCONF"),
                    String::from("ACK"),
                    format!(
                        "{}",
                        self.replication
                            .as_ref()
                            .map(|r| r.replication_offset)
                            .unwrap_or(0)
                    ),
                ]);
                println!("Sending {message:?}");
                self.send_string(&message)
            }
            Some(option) if option == "ACK" => {
                if let Some(replication) = &mut self.replication {
                    replication.match_offsets();
                }
                return Some(PollResult::AckSuccessful);
            }
            _ => {
                self.send_string(&String::from("+OK\r\n"));
                return None;
            }
        };
        None
    }

    fn process_psync(&mut self, config: &Config) -> Option<PollResult> {
        self.stream.send(&format_string(Some(format!(
            "+FULLRESYNC {} {}",
            config.replication.replid, config.replication.repl_offset
        ))));

        let empty_db = fs::read("empty.rdb").ok()?;

        self.stream.send(&format!("${}\r\n", empty_db.len()));
        self.stream.send_raw(&empty_db);
        Some(PollResult::PromoteToReplica)
    }

    fn process_wait(&mut self, command: &[String]) -> Option<PollResult> {
        let expected_number_of_replicas = command.get(1).and_then(|n| n.parse::<usize>().ok());
        let ttl = command.get(2).and_then(|n| n.parse::<usize>().ok());
        match (expected_number_of_replicas, ttl) {
            (Some(n_replicas), ttl) => {
                self.wait_for_replication_ack = true;
                Some(PollResult::WaitForAcks(ReplicationCheckRequest {
                    number_of_replicas: n_replicas,
                    timeout: ttl,
                }))
            }
            _ => {
                println!("Cannot process invalid WAIT command: {command:?}");
                None
            }
        }
    }

    pub fn is_replication_offset_aligned(&self) -> bool {
        println!("{:?}", self.replication);
        self.replication
            .as_ref()
            .map(|r| !r.need_to_ask_for_replication_ack())
            .unwrap_or(false)
    }

    pub fn replication_handshake(
        &mut self,
        config: &Config,
        global_state: &mut Cell<Store>,
    ) -> Option<()> {
        let ReplicationRole::Replica((host, port)) = &config.replication.role else {
            return None;
        };
        println!("Starting replication handshake with {host}:{port}");
        println!("Enabling blocking behavior of the TCP stream");
        self.stream.set_stream_nonblocking_behavior(false);

        println!("Replication: sending PING");
        self.send_command(&vec![String::from("PING")]);
        println!("Replication: sending REPLCONF (1/2)");
        self.send_command(&vec![
            String::from("REPLCONF"),
            String::from("listening-port"),
            format!("{}", config.port),
        ]);

        println!("Replication: sending REPLCONF (2/2)");
        self.send_command(&vec![
            String::from("REPLCONF"),
            String::from("capa"),
            String::from("psync2"),
        ]);
        println!("Replication: sending PSYNC");
        self.send_string(&format_array(&vec![
            String::from("PSYNC"),
            String::from("?"),
            String::from("-1"),
        ]));

        println!("Disabling blocking behavior of the TCP stream");
        self.stream.set_stream_nonblocking_behavior(true);
        self.replication = Some(Replication {
            replication_offset: 0,
            last_offset_checked: 0,
        });
        self.poll(global_state, config);

        Some(())
    }
}
