#![allow(unused_imports)]
use std::{
    io::{ErrorKind, Read, Write},
    net::{TcpListener, TcpStream},
};

fn main() {
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    listener
        .set_nonblocking(true)
        .expect("Cannot put TCP listener in non-blocking mode");

    let mut tasks: Vec<PongTask> = Vec::new();

    loop {
        match listener.accept() {
            Ok((stream, _)) => {
                let task = PongTask::new(stream);
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
            (*task).poll();
        }

        tasks.retain(|task| task.active);
        // println!("End of main event loop");
    }
}

struct PongTask {
    stream: TcpStream,
    buffer: [u8; 128],
    active: bool,
}

impl PongTask {
    fn new(stream: TcpStream) -> PongTask {
        let buffer = [0u8; 128];
        stream
            .set_nonblocking(true)
            .expect("Cannot put TCP stream in non-blocking mode");
        PongTask {
            stream,
            buffer,
            active: true,
        }
    }

    fn poll(&mut self) {
        match self.stream.read(&mut self.buffer) {
            Ok(0) => {
                println!("Stream terminated");
                self.active = false;
            }
            Ok(_) => {
                // println!("Received stream buffer: {:?}", &self.buffer);
                match self.stream.write_all(b"+PONG\r\n") {
                    Ok(_) => {}
                    Err(_) => {}
                };
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
}
