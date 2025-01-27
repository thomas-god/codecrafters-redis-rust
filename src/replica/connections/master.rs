use std::{cell::Cell, net::TcpStream};

use crate::{
    config::{Config, ReplicationRole},
    connections::{
        fmt::{format_array, format_string},
        parser::{BufferType, Command, CommandVerb},
        stream::RedisStream,
        PollResult,
    },
    store::Store,
};

pub struct ReplicaToMasterConnection {
    stream: RedisStream<TcpStream>,
    pub active: bool,
    replication_offset: Option<usize>,
}

impl ReplicaToMasterConnection {
    pub fn new(
        config: &Config,
        global_state: &mut Cell<Store>,
    ) -> Option<ReplicaToMasterConnection> {
        let ReplicationRole::Replica((host, port)) = &config.replication.role else {
            return None;
        };
        let Some(master_connection) = TcpStream::connect(format!("{host}:{port}")).ok() else {
            panic!("Could not connect to master instance.");
        };
        let stream = RedisStream::new(master_connection);

        let mut connection = ReplicaToMasterConnection {
            stream,
            active: true,
            replication_offset: None,
        };

        connection.replication_handshake(config, global_state)?;

        Some(connection)
    }

    fn replication_handshake(
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
        self.replication_offset = Some(0);
        self.poll(global_state, config);

        Some(())
    }

    pub fn poll(&mut self, global_state: &mut Cell<Store>, config: &Config) -> Vec<PollResult> {
        match self.stream.read() {
            Some(elements) => {
                let mut poll_results: Vec<PollResult> = vec![];
                for element in elements {
                    match element {
                        BufferType::Command(cmd) => {
                            if let Some(result) = self.process_command(&cmd, global_state, config) {
                                poll_results.push(result);
                            }
                            self.track_replication_offset(cmd.cmd);
                        }
                        elem => println!("Nothing to do for: {elem:?}"),
                    }
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
        if let Some(ref mut replication_offset) = self.replication_offset {
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
        self.stream.send_string(message);
    }

    fn process_command(
        &mut self,
        cmd: &Command,
        global_state: &mut Cell<Store>,
        config: &Config,
    ) -> Option<PollResult> {
        println!("Processing command: {cmd:?}");
        let Command { verb, cmd } = cmd;
        match verb {
            CommandVerb::ECHO => self.process_echo(cmd),
            CommandVerb::SET => self.process_set(cmd, global_state),
            CommandVerb::GET => self.process_get(cmd, global_state),
            CommandVerb::CONFIG => self.process_config(cmd, config),
            CommandVerb::KEYS => self.process_keys(cmd, global_state),
            CommandVerb::INFO => self.process_info(cmd, config),
            CommandVerb::REPLCONF => self.process_replconf(cmd),
            unsupported_verb => self.log_unsupported_verb(unsupported_verb),
        }
    }

    fn process_echo(&mut self, command: &[String]) -> Option<PollResult> {
        if let Some(message) = command.get(1) {
            let message = format!("${}\r\n{}\r\n", message.len(), message);
            self.stream.send_string(&message);
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
        global_state.get_mut().set_string(key, value, ttl);
        Some(PollResult::Write(format_array(&command.to_vec())))
    }

    fn process_get(
        &mut self,
        command: &[String],
        global_state: &mut Cell<Store>,
    ) -> Option<PollResult> {
        let key = command.get(1)?;
        self.stream
            .send_string(&format_string(global_state.get_mut().get_string(key)));
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
                self.stream.send_string(&message);
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
        self.stream.send_string(&response);
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
                self.stream.send_string(&format_string(Some(response)));
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
                    format!("{}", self.replication_offset.unwrap_or(0)),
                ]);
                println!("Sending {message:?}");
                self.send_string(&message)
            }
            _ => {
                self.send_string(&String::from("+OK\r\n"));
                return None;
            }
        };
        None
    }

    fn log_unsupported_verb(&self, verb: &CommandVerb) -> Option<PollResult> {
        println!("{verb:?} not implemented for Replica to Master connection");
        None
    }
}
