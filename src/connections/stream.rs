use std::io::{Read, Write};

use crate::connections::parser::parse_buffer;

use super::parser::BufferType;

pub struct RedisStream<S: Write + Read> {
    stream: S,
}

impl<S: Write + Read> RedisStream<S> {
    pub fn read(&mut self) -> Vec<BufferType> {
        let mut buffer = [0u8; 256];
        let Ok(n) = self.stream.read(&mut buffer) else {
            return vec![];
        };
        let Some(elements) = parse_buffer(&buffer[0..n]) else {
            return vec![];
        };

        elements
    }

    pub fn send(&mut self, message: &str) {
        if let Err(err) = self.stream.write_all(message.as_bytes()) {
            println!("Error when trying to send string {:?}: {:?}", &message, err);
        }
    }

    #[cfg(test)]
    fn get_stream(&self) -> &S {
        &self.stream
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use super::RedisStream;
    use crate::connections::parser::BufferType;

    #[test]
    fn test_parse_simple_string() {
        let stream = VecDeque::from(String::from("+OK\r\n").into_bytes());

        let mut redis_stream = RedisStream { stream };
        let expected_response = vec![BufferType::String(String::from("OK"))];
        assert_eq!(redis_stream.read(), expected_response)
    }

    #[test]
    fn test_send_string() {
        let stream: VecDeque<u8> = VecDeque::new();

        let mut redis_stream = RedisStream { stream };

        let message = String::from("+OK\r\n");
        redis_stream.send(&message);

        assert_eq!(
            redis_stream.get_stream().as_slices(),
            (message.as_bytes(), &[] as &[u8])
        );
    }
}
