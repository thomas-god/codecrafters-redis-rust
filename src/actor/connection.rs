use std::{
    net::TcpStream,
    sync::mpsc::{channel, Receiver, Sender},
};

use crate::connections::stream::RedisStream;

use super::{ConnectionMessage, StoreMessage};

pub struct Connection {
    stream: RedisStream<TcpStream>,
    tx_store: Sender<StoreMessage>,
    tx: Sender<ConnectionMessage>,
    rx: Receiver<ConnectionMessage>,
}

impl Connection {
    pub fn new(stream: RedisStream<TcpStream>, tx_store: Sender<StoreMessage>) -> Connection {
        let (tx, rx) = channel();
        Connection {
            stream,
            tx_store,
            tx,
            rx,
        }
    }

    pub fn get_tx(&self) -> Sender<ConnectionMessage> {
        self.tx.clone()
    }

    pub fn poll(&mut self) {
        if let Some(messages) = self.stream.read() {
            for msg in messages {
                println!("Received message: {msg:?}");
                self.tx_store
                    .send(StoreMessage::NewBuffer {
                        value: msg,
                        tx_back: self.tx.clone(),
                    })
                    .unwrap();
            }
        }

        while let Ok(msg) = self.rx.try_recv() {
            println!("Message to send: {msg:?}");
            match msg {
                ConnectionMessage::SendString(msg) => self.stream.send_string(&msg),
                ConnectionMessage::SendBytes(bytes) => self.stream.send_bytes(&bytes),
            }
        }
    }
}
