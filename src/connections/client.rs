use std::{
    cell::Cell,
    collections::HashMap,
    fs,
    io::{ErrorKind, Read, Write},
    net::TcpStream,
    str::from_utf8,
};

use chrono::naive;

use crate::{
    config::{Config, ReplicationRole},
    fmt::{format_command, format_string},
    parser::{parse_command, RESPSimpleType},
    store::Store,
};

use super::PollResult;

pub struct ClientConnection {
    stream: TcpStream,
    buffer: [u8; 128],
    pub active: bool,
    pub replica: bool,
}

impl ClientConnection {
    pub fn new(stream: TcpStream) -> ClientConnection {
        let buffer = [0u8; 128];
        stream
            .set_nonblocking(true)
            .expect("Cannot put TCP stream in non-blocking mode");
        ClientConnection {
            stream,
            buffer,
            active: true,
            replica: false,
        }
    }

    pub fn poll(&mut self, global_state: &mut Cell<Store>, config: &Config) -> Option<PollResult> {
        match self.stream.read(&mut self.buffer) {
            Ok(0) => {
                println!("Stream terminated");
                self.active = false;
                None
            }
            Ok(n) => {
                println!(
                    "Received stream buffer: {:?}",
                    from_utf8(&self.buffer[..n]).unwrap()
                );
                let result = self.process_buffer(n, global_state, config);
                if let Some(PollResult::PromoteToReplica) = result {
                    println!("Promoting connection to replica state, will propagate future writes to it.");
                    self.replica = true;
                };
                result
            }
            Err(ref e) if e.kind() == ErrorKind::WouldBlock => {
                // println!("No data in stream");
                None
            }
            Err(err) => {
                println!("Stream terminated with err: {}", err);
                self.active = false;
                None
            }
        }
    }

    pub fn send_command(&mut self, cmd: &str) {
        let _ = self.stream.write_all(cmd.as_bytes());
    }

    fn process_buffer(
        &mut self,
        n: usize,
        global_state: &mut Cell<Store>,
        config: &Config,
    ) -> Option<PollResult> {
        if let Some(command) = parse_command(&self.buffer[..n]) {
            if let Some(RESPSimpleType::String(verb)) = command.first() {
                return match verb.as_str() {
                    "PING" => self.process_ping(),
                    "ECHO" => self.process_echo(&command),
                    "SET" => self.process_set(&command, global_state),
                    "GET" => self.process_get(&command, global_state),
                    "CONFIG" => self.process_config(&command, config),
                    "KEYS" => self.process_keys(&command, global_state),
                    "INFO" => self.process_info(&command, config),
                    "REPLCONF" => self.process_replconf(),
                    "PSYNC" => self.process_psync(config),
                    _ => panic!(),
                };
            }
        }
        None
    }

    fn process_ping(&mut self) -> Option<PollResult> {
        self.stream
            .write_all(String::from("+PONG\r\n").as_bytes())
            .ok()?;
        None
    }

    fn process_echo(&mut self, command: &[RESPSimpleType]) -> Option<PollResult> {
        if let RESPSimpleType::String(message) = command.get(1).unwrap() {
            self.stream
                .write_all(format!("${}\r\n{}\r\n", message.len(), message).as_bytes())
                .ok()?;
            None
        } else {
            None
        }
    }

    fn process_set(
        &mut self,
        command: &[RESPSimpleType],
        global_state: &mut Cell<Store>,
    ) -> Option<PollResult> {
        let (Some(RESPSimpleType::String(key)), Some(RESPSimpleType::String(value))) =
            (command.get(1), command.get(2))
        else {
            return None;
        };

        let option = match command.get(3) {
            Some(RESPSimpleType::String(option)) => Some(option),
            _ => None,
        };
        let option_value: Option<usize> = match command.get(4) {
            Some(RESPSimpleType::String(option)) => option.parse::<usize>().ok(),
            _ => None,
        };
        let ttl = match (option, option_value) {
            (Some(cmd), Some(cmd_value)) if cmd == "px" => Some(cmd_value),
            _ => None,
        };

        println!("Setting {}: {}", key, value);
        global_state.get_mut().set(key, value, ttl);
        self.stream
            .write_all(String::from("+OK\r\n").as_bytes())
            .ok()?;
        Some(PollResult::Write(format_command(command)))
    }

    fn process_get(
        &mut self,
        command: &[RESPSimpleType],
        global_state: &mut Cell<Store>,
    ) -> Option<PollResult> {
        let Some(RESPSimpleType::String(key)) = command.get(1) else {
            return None;
        };
        self.stream
            .write_all(format_string(global_state.get_mut().get(key)).as_bytes())
            .ok()?;
        None
    }

    fn process_config(
        &mut self,
        command: &[RESPSimpleType],
        config: &Config,
    ) -> Option<PollResult> {
        match command.get(1) {
            Some(RESPSimpleType::String(action)) if *action == "GET" => {
                let Some(RESPSimpleType::String(key)) = command.get(2) else {
                    return None;
                };
                let value = config.get_arg(key)?.clone();
                self.stream
                    .write_all(
                        format!(
                            "*2\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                            key.len(),
                            key,
                            value.len(),
                            value
                        )
                        .as_bytes(),
                    )
                    .ok()?;
                None
            }
            _ => panic!(),
        }
    }

    fn process_keys(
        &mut self,
        _command: &[RESPSimpleType],
        global_state: &mut Cell<Store>,
    ) -> Option<PollResult> {
        let mut response = String::new();
        let keys = global_state.get_mut().get_keys();
        response.push_str(&format!("*{}\r\n", keys.len()));
        for key in keys {
            response.push_str(&format!("${}\r\n{}\r\n", key.len(), key));
        }
        self.stream.write_all(response.as_bytes()).ok()?;
        None
    }

    fn process_info(&mut self, command: &[RESPSimpleType], config: &Config) -> Option<PollResult> {
        match command.get(1) {
            Some(RESPSimpleType::String(section)) if *section == "replication" => {
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

                self.stream
                    .write_all(format_string(Some(response)).as_bytes())
                    .ok()?;
                None
            }
            _ => panic!(),
        }
    }

    fn process_replconf(&mut self) -> Option<PollResult> {
        self.stream
            .write_all(String::from("+OK\r\n").as_bytes())
            .ok()?;
        None
    }

    fn process_psync(&mut self, config: &Config) -> Option<PollResult> {
        self.stream
            .write_all(
                format_string(Some(format!(
                    "FULLRESYNC {} {}",
                    config.replication.replid, config.replication.repl_offset
                )))
                .as_bytes(),
            )
            .ok()?;

        let empty_db = fs::read("empty.rdb").ok()?;

        self.stream
            .write_all(format!("${}\r\n", empty_db.len()).as_bytes())
            .ok()?;
        self.stream.write_all(&empty_db).ok()?;
        Some(PollResult::PromoteToReplica)
    }
}
