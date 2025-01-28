use std::{
    net::TcpStream,
    sync::mpsc::{channel, Receiver, Sender},
};

use uuid::Uuid;

use crate::{
    actor::{ConnectionID, ConnectionMessage, StoreMessage},
    connection::stream::RedisStream,
};

pub mod fmt;
pub mod parser;
pub mod stream;

pub struct Connection {
    stream: RedisStream<TcpStream>,
    tx_store: Sender<StoreMessage>,
    tx: Sender<ConnectionMessage>,
    rx: Receiver<ConnectionMessage>,
    connection_id: ConnectionID,
}

impl Connection {
    pub fn new(stream: RedisStream<TcpStream>, tx_store: Sender<StoreMessage>) -> Connection {
        let (tx, rx) = channel();
        let connection_id = Uuid::new_v4().to_string();
        Connection {
            stream,
            tx_store,
            tx,
            rx,
            connection_id,
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
                        connection_id: self.connection_id.clone(),
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
