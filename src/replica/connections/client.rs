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

pub struct ReplicaToClientConnection {
    stream: RedisStream<TcpStream>,
    pub active: bool,
}

impl ReplicaToClientConnection {
    pub fn new(stream: TcpStream) -> ReplicaToClientConnection {
        stream
            .set_nonblocking(true)
            .expect("Cannot put TCP stream in non-blocking mode");
        let stream = RedisStream::new(stream);
        ReplicaToClientConnection {
            stream,
            active: true,
        }
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

    pub fn send_command(&mut self, command: &Vec<String>) {
        let message = format_array(command);
        self.send_string(&message);
        self.stream.read();
    }

    pub fn send_string(&mut self, message: &str) {
        self.stream.send(message);
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
            CommandVerb::PING => self.process_ping(),
            CommandVerb::ECHO => self.process_echo(cmd),
            CommandVerb::GET => self.process_get(cmd, global_state),
            CommandVerb::CONFIG => self.process_config(cmd, config),
            CommandVerb::KEYS => self.process_keys(cmd, global_state),
            CommandVerb::INFO => self.process_info(cmd, config),
            unsupported_verb => self.process_unsupported_verb(unsupported_verb),
        }
    }

    fn process_ping(&mut self) -> Option<PollResult> {
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

    fn process_get(
        &mut self,
        command: &[String],
        global_state: &mut Cell<Store>,
    ) -> Option<PollResult> {
        let key = command.get(1)?;
        self.stream
            .send(&format_string(global_state.get_mut().get_string(key)));
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

    fn process_unsupported_verb(&self, verb: &CommandVerb) -> Option<PollResult> {
        println!("{verb:?} not implemented for Replica to Client connection");
        None
    }
}
