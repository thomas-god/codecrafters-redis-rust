use std::{
    cell::Cell,
    collections::HashMap,
    fs,
    io::{ErrorKind, Read, Write},
    net::TcpStream,
    str::from_utf8,
    task::Poll,
};

use chrono::naive;

use crate::{
    config::{Config, ReplicationRole},
    fmt::{format_command, format_string},
    parser::{parse_command, RESPSimpleType},
    send_command,
    store::Store,
};

use super::PollResult;

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum ConnectionRole {
    Master,
    Client,
    Replica,
}

pub struct ClientConnection {
    stream: TcpStream,
    buffer: [u8; 512],
    pub active: bool,
    pub connected_with: ConnectionRole,
}

impl ClientConnection {
    pub fn new(stream: TcpStream) -> ClientConnection {
        let buffer = [0u8; 512];
        stream
            .set_nonblocking(true)
            .expect("Cannot put TCP stream in non-blocking mode");
        ClientConnection {
            stream,
            buffer,
            active: true,
            connected_with: ConnectionRole::Client,
        }
    }

    pub fn poll(&mut self, global_state: &mut Cell<Store>, config: &Config) -> Vec<PollResult> {
        match self.stream.read(&mut self.buffer) {
            Ok(0) => {
                println!("Stream terminated");
                self.active = false;
                Vec::new()
            }
            Ok(n) => {
                let data = match from_utf8(&self.buffer) {
                    Ok(data) => data,
                    Err(err) => from_utf8(&self.buffer[0..err.valid_up_to()]).unwrap(),
                };
                println!("Received stream buffer ({n} bytes): {:?}", data);
                let results = self.process_buffer(n, global_state, config);
                if results.iter().any(|r| *r == PollResult::PromoteToReplica) {
                    println!(
                        "Promoting connection to replica role, will propagate future writes to it."
                    );
                    self.connected_with = ConnectionRole::Replica;
                }
                results
            }
            Err(ref e) if e.kind() == ErrorKind::WouldBlock => {
                // println!("No data in stream");
                Vec::new()
            }
            Err(err) => {
                println!("Stream terminated with err: {}", err);
                self.active = false;
                Vec::new()
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
    ) -> Vec<PollResult> {
        let commands = parse_command(&self.buffer[..n]);
        let mut results: Vec<PollResult> = Vec::new();
        for command in commands {
            println!("List of values: {command:?}");
            if let Some(RESPSimpleType::String(verb)) = command.first() {
                let res = match verb.as_str() {
                    "PING" => self.process_ping(),
                    "ECHO" => self.process_echo(&command),
                    "SET" => self.process_set(&command, global_state),
                    "GET" => self.process_get(&command, global_state),
                    "CONFIG" => self.process_config(&command, config),
                    "KEYS" => self.process_keys(&command, global_state),
                    "INFO" => self.process_info(&command, config),
                    "REPLCONF" => self.process_replconf(),
                    "PSYNC" => self.process_psync(config),
                    v => {
                        println!("Found invalid verb to process: {v}");
                        None
                    }
                };
                if let Some(res) = res {
                    results.push(res);
                }
            } else {
                println!("Command seems empty.")
            }
        }
        results
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
        if let ConnectionRole::Client = &self.connected_with {
            self.stream
                .write_all(String::from("+OK\r\n").as_bytes())
                .ok()?;
        };
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
                    "+FULLRESYNC {} {}",
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

    pub fn from_handshake(config: &Config) -> Option<ClientConnection> {
        if let ReplicationRole::Replica((host, port)) = &config.replication.role {
            println!("Starting replication handshake");
            let mut master_link = TcpStream::connect(format!("{host}:{port}")).ok()?;
            let mut buffer = [0u8; 2048];

            send_command(&mut master_link, &mut buffer, vec![String::from("PING")])?;
            send_command(
                &mut master_link,
                &mut buffer,
                vec![
                    String::from("REPLCONF"),
                    String::from("listening-port"),
                    format!("{}", config.port),
                ],
            )?;
            send_command(
                &mut master_link,
                &mut buffer,
                vec![
                    String::from("REPLCONF"),
                    String::from("capa"),
                    String::from("psync2"),
                ],
            )?;
            let n = send_command(
                &mut master_link,
                &mut buffer,
                vec![String::from("PSYNC"), String::from("?"), String::from("-1")],
            )?;
            println!("Received {n} bytes from PSYNC cmd");

            let data = match from_utf8(&buffer[0..n]) {
                Ok(data) => data,
                Err(err) => {
                    println!(
                        "Expected {n} bytes, but buffer was valid up to {} bytes",
                        err.valid_up_to()
                    );
                    from_utf8(&buffer[0..err.valid_up_to()]).unwrap()
                }
            };
            println!("Received RDB file: {}", data);

            // let n = master_link.read(&mut buffer).ok().unwrap();
            // println!("Received {n} bytes");
            // let rdb_content = from_utf8(&buffer[0..n]).unwrap();
            // println!("Received write commands: {}", rdb_content);
            println!("Handshake successfully done");
            // let mut tmp = [0u8;128];
            // tmp.copy_from_slice(&buffer[0..128]);
            return Some(ClientConnection {
                stream: master_link,
                active: true,
                buffer: [0u8; 512],
                connected_with: ConnectionRole::Master,
            });
        };
        None
    }
}
