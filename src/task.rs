use std::{
    cell::Cell,
    collections::HashMap,
    io::{ErrorKind, Read, Write},
    net::TcpStream,
    str::from_utf8,
};

use crate::{
    parser::{parse_command, RESPSimpleType},
    store::Store,
};

pub struct RedisTask {
    stream: TcpStream,
    buffer: [u8; 128],
    pub active: bool,
}

impl RedisTask {
    pub fn new(stream: TcpStream) -> RedisTask {
        let buffer = [0u8; 128];
        stream
            .set_nonblocking(true)
            .expect("Cannot put TCP stream in non-blocking mode");
        RedisTask {
            stream,
            buffer,
            active: true,
        }
    }

    pub fn poll(&mut self, global_state: &mut Cell<Store>, config: &HashMap<String, String>) {
        match self.stream.read(&mut self.buffer) {
            Ok(0) => {
                println!("Stream terminated");
                self.active = false;
            }
            Ok(n) => {
                println!(
                    "Received stream buffer: {:?}",
                    from_utf8(&self.buffer[..n]).unwrap()
                );
                self.process_buffer(n, global_state, config);
            }
            Err(ref e) if e.kind() == ErrorKind::WouldBlock => {
                // println!("No data in stream");
            }
            Err(err) => {
                println!("Stream terminated with err: {}", err);
                self.active = false;
            }
        }
    }

    fn process_buffer(
        &mut self,
        n: usize,
        global_state: &mut Cell<Store>,
        config: &HashMap<String, String>,
    ) {
        if let Some(command) = parse_command(&self.buffer[..n]) {
            if let Some(RESPSimpleType::String(verb)) = command.first() {
                let response = match *verb {
                    "PING" => self.process_ping(),
                    "ECHO" => self.process_echo(&command),
                    "SET" => self.process_set(&command, global_state),
                    "GET" => self.process_get(&command, global_state),
                    "CONFIG" => self.process_config(&command, config),
                    _ => panic!(),
                };
                if let Some(response) = response {
                    let _ = self.stream.write_all(response.as_bytes());
                }
            }
        }
    }

    fn process_ping(&self) -> Option<String> {
        Some(String::from("+PONG\r\n"))
    }

    fn process_echo(&self, command: &[RESPSimpleType]) -> Option<String> {
        if let RESPSimpleType::String(message) = command.get(1).unwrap() {
            Some(format!("${}\r\n{}\r\n", message.len(), message))
        } else {
            None
        }
    }

    fn process_set(
        &self,
        command: &[RESPSimpleType],
        global_state: &mut Cell<Store>,
    ) -> Option<String> {
        let (Some(RESPSimpleType::String(key)), Some(RESPSimpleType::String(value))) =
            (command.get(1), command.get(2))
        else {
            return None;
        };

        let option = match command.get(3) {
            Some(RESPSimpleType::String(option)) => Some(option),
            _ => None,
        };
        let option_value = match command.get(4) {
            Some(RESPSimpleType::String(option)) => option.parse::<usize>().ok(),
            _ => None,
        };
        let ttl = match (option, option_value) {
            (Some(cmd), Some(cmd_value)) if cmd == &"px" => Some(cmd_value),
            _ => None,
        };

        println!("{}: {}", key, value);
        global_state.get_mut().set(key, value, ttl);
        Some(String::from("+OK\r\n"))
    }

    fn process_get(
        &self,
        command: &[RESPSimpleType],
        global_state: &mut Cell<Store>,
    ) -> Option<String> {
        let Some(RESPSimpleType::String(key)) = command.get(1) else {
            return None;
        };
        let key = String::from(*key);

        let response = match global_state.get_mut().get(&key) {
            Some(value) => format!("${}\r\n{}\r\n", value.len(), value),
            None => String::from("$-1\r\n"),
        };
        Some(response)
    }

    fn process_config(
        &self,
        command: &[RESPSimpleType],
        config: &HashMap<String, String>,
    ) -> Option<String> {
        match command.get(1) {
            Some(RESPSimpleType::String(action)) if *action == "GET" => {
                let Some(RESPSimpleType::String(key)) = command.get(2) else {
                    return None;
                };
                let key = String::from(*key);
                let value = config.get(&key)?.clone();
                Some(format!(
                    "*2\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                    key.len(),
                    key,
                    value.len(),
                    value
                ))
            }
            _ => panic!(),
        }
    }
}
