use std::sync::mpsc::Sender;

use crate::connection::parser::BufferType;

pub mod master;
pub mod replica;

pub type ConnectionID = String;

#[derive(Debug)]
pub enum StoreMessage {
    NewBuffer {
        value: BufferType,
        tx_back: Sender<ConnectionMessage>,
        connection_id: ConnectionID,
    },
}

#[derive(Debug)]
pub enum ConnectionMessage {
    SendString(String),
    SendBytes(Vec<u8>),
}
