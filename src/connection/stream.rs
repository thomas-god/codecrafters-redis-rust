use std::{
    io::{ErrorKind, Read, Write},
    net::TcpStream,
};

use super::parser::{parse_buffer, BufferType};

pub struct RedisStream<S: Write + Read> {
    stream: S,
    buffer: [u8; 512],
}

impl<S: Write + Read> RedisStream<S> {
    pub fn new(stream: S) -> Self {
        let buffer = [0u8; 512];

        Self { stream, buffer }
    }

    pub fn read(&mut self) -> Option<Vec<BufferType>> {
        match self.stream.read(&mut self.buffer) {
            Ok(0) => None,
            Ok(n) => parse_buffer(&self.buffer[0..n]),
            Err(ref e) if e.kind() == ErrorKind::WouldBlock => Some(Vec::new()),
            Err(err) => {
                println!("Stream terminated with err: {}", err);
                Some(Vec::new())
            }
        }
    }

    pub fn send_string(&mut self, message: &str) {
        if let Err(err) = self.stream.write_all(message.as_bytes()) {
            println!("Error when trying to send string {:?}: {:?}", &message, err);
        }
    }

    pub fn send_bytes(&mut self, bytes: &[u8]) {
        if let Err(err) = self.stream.write_all(bytes) {
            println!("Error when trying to send bytes {:?}: {:?}", &bytes, err);
        }
    }

    #[cfg(test)]
    fn get_stream(&self) -> &S {
        &self.stream
    }
}

impl RedisStream<TcpStream> {
    pub fn set_stream_nonblocking_behavior(&mut self, non_blocking: bool) {
        self.stream
            .set_nonblocking(non_blocking)
            .expect("Cannot put TCP stream in non-blocking mode");
    }
}

#[cfg(test)]
mod tests {
    use super::RedisStream;
    use crate::connection::stream::BufferType;
    use std::collections::VecDeque;

    #[test]
    fn test_parse_simple_string() {
        let stream = VecDeque::from(String::from("+OK\r\n").into_bytes());

        let mut redis_stream = RedisStream::new(stream);
        let expected_response = vec![BufferType::String(String::from("OK"))];
        assert_eq!(redis_stream.read(), Some(expected_response))
    }

    #[test]
    fn test_send_string() {
        let stream: VecDeque<u8> = VecDeque::new();

        let mut redis_stream = RedisStream::new(stream);

        let message = String::from("+OK\r\n");
        redis_stream.send_string(&message);

        assert_eq!(
            redis_stream.get_stream().as_slices(),
            (message.as_bytes(), &[] as &[u8])
        );
    }

    #[test]
    fn should_exit_when_stream_empty_on_read() {
        let stream: VecDeque<u8> = VecDeque::new();

        let mut redis_stream = RedisStream::new(stream);

        assert_eq!(redis_stream.read(), None)
    }
}
