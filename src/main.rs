#![allow(unused_imports)]
use parser::{parse_command, RESPSimpleType};
use std::{
    arch::global_asm,
    cell::Cell,
    collections::HashMap,
    io::{ErrorKind, Read, Write},
    net::{TcpListener, TcpStream},
    os::fd,
    process::CommandArgs,
    str::from_utf8,
};

pub mod parser;

fn main() {
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    listener
        .set_nonblocking(true)
        .expect("Cannot put TCP listener in non-blocking mode");

    let mut tasks: Vec<RedisTask> = Vec::new();
    let mut store = Cell::new(HashMap::new());

    loop {
        match listener.accept() {
            Ok((stream, _)) => {
                let task = RedisTask::new(stream);
                tasks.push(task);
            }
            Err(ref e) if e.kind() == ErrorKind::WouldBlock => {
                // println!("No new connection");
            }
            Err(err) => {
                println!("An error occured: {}", err);
            }
        }

        for task in tasks.iter_mut() {
            (*task).poll(&mut store);
        }

        tasks.retain(|task| task.active);
    }
}

struct RedisTask {
    stream: TcpStream,
    buffer: [u8; 128],
    active: bool,
}

impl RedisTask {
    fn new(stream: TcpStream) -> RedisTask {
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

    fn poll(&mut self, global_state: &mut Cell<HashMap<String, String>>) {
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
                self.process_buffer(n, global_state);
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

    fn process_buffer(&mut self, n: usize, global_state: &mut Cell<HashMap<String, String>>) {
        if let Some(command) = parse_command(&self.buffer[..n]) {
            if let Some(RESPSimpleType::String(verb)) = command.first() {
                let response = match *verb {
                    "PING" => self.process_ping(),
                    "ECHO" => self.process_echo(&command),
                    "SET" => self.process_set(&command, global_state),
                    "GET" => self.process_get(&command, global_state),
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

    fn process_echo(&self, command: &Vec<RESPSimpleType>) -> Option<String> {
        if let RESPSimpleType::String(message) = command.get(1).unwrap() {
            Some(format!("${}\r\n{}\r\n", message.len(), message))
        } else {
            None
        }
    }

    fn process_set(
        &self,
        command: &Vec<RESPSimpleType>,
        global_state: &mut Cell<HashMap<String, String>>,
    ) -> Option<String> {
        if let (RESPSimpleType::String(key), RESPSimpleType::String(value)) =
            (command.get(1).unwrap(), command.get(2).unwrap())
        {
            println!("{}: {}", key, value);
            global_state
                .get_mut()
                .insert(String::from(*key), String::from(*value));
        }
        Some(String::from("+OK\r\n"))
    }

    fn process_get(
        &self,
        command: &Vec<RESPSimpleType>,
        globale_state: &mut Cell<HashMap<String, String>>,
    ) -> Option<String> {
        if let Some(RESPSimpleType::String(key)) = command.get(1) {
            let key = String::from(*key);
            if let Some(&ref value) = globale_state.get_mut().get(&key) {
                let response = format!("${}\r\n{}\r\n", value.len(), value);
                Some(response)
            } else {
                None
            }
        } else {
            None
        }
    }
}
