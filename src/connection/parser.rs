use std::str::from_utf8;

use itertools::Itertools;

#[derive(Debug, PartialEq)]
pub enum BufferType {
    String(String),
    DBFile(Vec<u8>),
    Command(Command),
}

#[derive(Debug, PartialEq)]
pub struct Command {
    pub verb: CommandVerb,
    pub cmd: Vec<String>,
}

#[derive(Debug, PartialEq)]
pub enum CommandVerb {
    PING,
    ECHO,
    SET,
    GET,
    TYPE,
    XADD,
    XRANGE,
    XREAD,
    CONFIG,
    KEYS,
    INFO,
    REPLCONF,
    PSYNC,
    WAIT,
    INCR,
    MULTI,
}

impl TryFrom<String> for CommandVerb {
    type Error = &'static str;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        let value = value.to_uppercase();
        match value.as_str() {
            "PING" => Ok(Self::PING),
            "ECHO" => Ok(Self::ECHO),
            "SET" => Ok(Self::SET),
            "GET" => Ok(Self::GET),
            "TYPE" => Ok(Self::TYPE),
            "XADD" => Ok(Self::XADD),
            "XRANGE" => Ok(Self::XRANGE),
            "XREAD" => Ok(Self::XREAD),
            "CONFIG" => Ok(Self::CONFIG),
            "KEYS" => Ok(Self::KEYS),
            "INFO" => Ok(Self::INFO),
            "REPLCONF" => Ok(Self::REPLCONF),
            "PSYNC" => Ok(Self::PSYNC),
            "WAIT" => Ok(Self::WAIT),
            "INCR" => Ok(Self::INCR),
            "MULTI" => Ok(Self::MULTI),
            _ => Err("Unsupported command verb"),
        }
    }
}

pub fn parse_buffer(buffer: &[u8]) -> Option<Vec<BufferType>> {
    let mut buffer_iter = buffer.iter();

    let mut elements: Vec<BufferType> = Vec::new();
    while let Some(byte) = buffer_iter.next() {
        match byte {
            b'+' => {
                if let Some(word) = parse_simple_string(&mut buffer_iter) {
                    elements.push(word);
                }
            }
            b'$' => {
                if let Some(word) = parse_bulk_string_like(&mut buffer_iter) {
                    elements.push(word);
                }
            }
            b'*' => {
                if let Some(array) = parse_array_into_command(&mut buffer_iter) {
                    elements.push(array);
                }
            }
            _ => panic!(),
        }
    }
    Some(elements)
}

fn parse_simple_string(iterator: &mut std::slice::Iter<'_, u8>) -> Option<BufferType> {
    let bytes = find_until_next_delimiter(iterator);
    from_utf8(&bytes)
        .map(|word| BufferType::String(word.to_string()))
        .ok()
}

fn parse_bulk_string_like(iterator: &mut std::slice::Iter<'_, u8>) -> Option<BufferType> {
    let len = from_utf8(&find_until_next_delimiter(iterator))
        .ok()
        .and_then(|bytes| bytes.parse::<usize>().ok())?;
    let mut bytes: Vec<u8> = Vec::new();
    for _ in 0..len {
        let _ = iterator.next().map(|byte| bytes.push(*byte));
    }

    // Check if the next 2 bytes are a delimiter (it's a bulk string, consume those 2 bytes) or not
    // (it's a DB file, do not consume those 2 bytes)
    let mut iter_peek = iterator.clone().tuple_windows::<(_, _)>().peekable();
    match iter_peek.peek() {
        Some((first, second)) if (**first, **second) == (b'\r', b'\n') => {
            iterator.next();
            iterator.next();
            from_utf8(&bytes)
                .ok()
                .map(|word| BufferType::String(word.to_string()))
        }
        _ => Some(BufferType::DBFile(bytes)),
    }
}

fn parse_array_into_command(iterator: &mut std::slice::Iter<'_, u8>) -> Option<BufferType> {
    let len = from_utf8(&find_until_next_delimiter(iterator))
        .ok()
        .and_then(|bytes| bytes.parse::<usize>().ok())?;

    let mut elements: Vec<String> = Vec::new();
    for _ in 0..len {
        iterator.next();
        if let Some(BufferType::String(elem)) = parse_bulk_string_like(iterator) {
            elements.push(elem);
        }
    }

    let verb = CommandVerb::try_from(elements.first().unwrap_or(&String::from("")).clone()).ok()?;

    Some(BufferType::Command(Command {
        cmd: elements,
        verb,
    }))
}

fn find_until_next_delimiter<'a, I>(iterator: &mut I) -> Vec<u8>
where
    I: Iterator<Item = &'a u8>,
{
    let mut elements: Vec<u8> = vec![];
    let double_iterator = iterator.by_ref().tuple_windows::<(_, _)>();
    for (first, second) in double_iterator {
        if (*first, *second) == (b'\r', b'\n') {
            break;
        };
        elements.push(*first);
    }
    elements
}

#[cfg(test)]
mod tests {
    use super::{parse_buffer, BufferType, Command, CommandVerb};

    #[test]
    fn buffer_with_simple_string() {
        let buffer = String::from("+OK\r\n").into_bytes();
        let expected_response = vec![BufferType::String(String::from("OK"))];
        assert_eq!(parse_buffer(&buffer), Some(expected_response));
    }

    #[test]
    fn buffer_with_2_simple_strings() {
        let buffer = String::from("+OK\r\n+hello\r\n").into_bytes();
        let expected_response = vec![
            BufferType::String(String::from("OK")),
            BufferType::String(String::from("hello")),
        ];
        assert_eq!(parse_buffer(&buffer), Some(expected_response));
    }

    #[test]
    fn buffer_with_bulk_string() {
        let buffer = String::from("$4\r\nPING\r\n").into_bytes();
        let expected_response = vec![BufferType::String(String::from("PING"))];
        assert_eq!(parse_buffer(&buffer), Some(expected_response));
    }

    #[test]
    fn buffer_with_db_file_string() {
        let buffer = vec![
            36, 56, 56, 13, 10, 82, 69, 68, 73, 83, 48, 48, 49, 49, 250, 9, 114, 101, 100, 105,
            115, 45, 118, 101, 114, 5, 55, 46, 50, 46, 48, 250, 10, 114, 101, 100, 105, 115, 45,
            98, 105, 116, 115, 192, 64, 250, 5, 99, 116, 105, 109, 101, 194, 109, 8, 188, 101, 250,
            8, 117, 115, 101, 100, 45, 109, 101, 109, 194, 176, 196, 16, 0, 250, 8, 97, 111, 102,
            45, 98, 97, 115, 101, 192, 0, 255, 240, 110, 59, 254, 192, 255, 90, 162,
        ];
        let db_file: Vec<u8> = Vec::from(&buffer[5..]);
        let expected_response = vec![BufferType::DBFile(db_file)];
        assert_eq!(parse_buffer(&buffer), Some(expected_response));
    }

    #[test]
    fn buffer_with_2_bulk_strings() {
        let buffer = String::from("$4\r\nPING\r\n$4\r\nPONG\r\n").into_bytes();
        let expected_response = vec![
            BufferType::String(String::from("PING")),
            BufferType::String(String::from("PONG")),
        ];
        assert_eq!(parse_buffer(&buffer), Some(expected_response));
    }

    #[test]
    fn test_buffer_with_array() {
        let buffer = String::from("*2\r\n$4\r\nECHO\r\n$4\r\ntoto\r\n").into_bytes();
        let expected_response = vec![BufferType::Command(Command {
            cmd: vec![String::from("ECHO"), String::from("toto")],
            verb: CommandVerb::ECHO,
        })];
        assert_eq!(parse_buffer(&buffer), Some(expected_response));
    }

    #[test]
    fn test_buffer_with_simple_string_and_db_file() {
        let buffer = vec![
            43, 70, 85, 76, 76, 82, 69, 83, 89, 78, 67, 32, 55, 53, 99, 100, 55, 98, 99, 49, 48,
            99, 52, 57, 48, 52, 55, 101, 48, 100, 49, 54, 51, 54, 54, 48, 102, 51, 98, 57, 48, 54,
            50, 53, 98, 49, 97, 102, 51, 49, 100, 99, 32, 48, 13, 10, 36, 56, 56, 13, 10, 82, 69,
            68, 73, 83, 48, 48, 49, 49, 250, 9, 114, 101, 100, 105, 115, 45, 118, 101, 114, 5, 55,
            46, 50, 46, 48, 250, 10, 114, 101, 100, 105, 115, 45, 98, 105, 116, 115, 192, 64, 250,
            5, 99, 116, 105, 109, 101, 194, 109, 8, 188, 101, 250, 8, 117, 115, 101, 100, 45, 109,
            101, 109, 194, 176, 196, 16, 0, 250, 8, 97, 111, 102, 45, 98, 97, 115, 101, 192, 0,
            255, 240, 110, 59, 254, 192, 255, 90, 162,
        ];
        let db_file = vec![
            82, 69, 68, 73, 83, 48, 48, 49, 49, 250, 9, 114, 101, 100, 105, 115, 45, 118, 101, 114,
            5, 55, 46, 50, 46, 48, 250, 10, 114, 101, 100, 105, 115, 45, 98, 105, 116, 115, 192,
            64, 250, 5, 99, 116, 105, 109, 101, 194, 109, 8, 188, 101, 250, 8, 117, 115, 101, 100,
            45, 109, 101, 109, 194, 176, 196, 16, 0, 250, 8, 97, 111, 102, 45, 98, 97, 115, 101,
            192, 0, 255, 240, 110, 59, 254, 192, 255, 90, 162,
        ];
        let expected_response = vec![
            BufferType::String(String::from(
                "FULLRESYNC 75cd7bc10c49047e0d163660f3b90625b1af31dc 0",
            )),
            BufferType::DBFile(db_file),
        ];
        assert_eq!(parse_buffer(&buffer), Some(expected_response));
    }

    #[test]
    fn test_buffer_with_simple_string_a_db_file_and_write_commands() {
        let buffer = vec![
            43, 70, 85, 76, 76, 82, 69, 83, 89, 78, 67, 32, 55, 53, 99, 100, 55, 98, 99, 49, 48,
            99, 52, 57, 48, 52, 55, 101, 48, 100, 49, 54, 51, 54, 54, 48, 102, 51, 98, 57, 48, 54,
            50, 53, 98, 49, 97, 102, 51, 49, 100, 99, 32, 48, 13, 10, 36, 56, 56, 13, 10, 82, 69,
            68, 73, 83, 48, 48, 49, 49, 250, 9, 114, 101, 100, 105, 115, 45, 118, 101, 114, 5, 55,
            46, 50, 46, 48, 250, 10, 114, 101, 100, 105, 115, 45, 98, 105, 116, 115, 192, 64, 250,
            5, 99, 116, 105, 109, 101, 194, 109, 8, 188, 101, 250, 8, 117, 115, 101, 100, 45, 109,
            101, 109, 194, 176, 196, 16, 0, 250, 8, 97, 111, 102, 45, 98, 97, 115, 101, 192, 0,
            255, 240, 110, 59, 254, 192, 255, 90, 162, 42, 51, 13, 10, 36, 51, 13, 10, 83, 69, 84,
            13, 10, 36, 51, 13, 10, 102, 111, 111, 13, 10, 36, 51, 13, 10, 49, 50, 51, 13, 10, 42,
            51, 13, 10, 36, 51, 13, 10, 83, 69, 84, 13, 10, 36, 51, 13, 10, 98, 97, 114, 13, 10,
            36, 51, 13, 10, 52, 53, 54, 13, 10, 42, 51, 13, 10, 36, 51, 13, 10, 83, 69, 84, 13, 10,
            36, 51, 13, 10, 98, 97, 122, 13, 10, 36, 51, 13, 10, 55, 56, 57, 13, 10,
        ];
        let db_file = vec![
            82, 69, 68, 73, 83, 48, 48, 49, 49, 250, 9, 114, 101, 100, 105, 115, 45, 118, 101, 114,
            5, 55, 46, 50, 46, 48, 250, 10, 114, 101, 100, 105, 115, 45, 98, 105, 116, 115, 192,
            64, 250, 5, 99, 116, 105, 109, 101, 194, 109, 8, 188, 101, 250, 8, 117, 115, 101, 100,
            45, 109, 101, 109, 194, 176, 196, 16, 0, 250, 8, 97, 111, 102, 45, 98, 97, 115, 101,
            192, 0, 255, 240, 110, 59, 254, 192, 255, 90, 162,
        ];
        let expected_response = vec![
            BufferType::String(String::from(
                "FULLRESYNC 75cd7bc10c49047e0d163660f3b90625b1af31dc 0",
            )),
            BufferType::DBFile(db_file),
            BufferType::Command(Command {
                cmd: vec![
                    String::from("SET"),
                    String::from("foo"),
                    String::from("123"),
                ],
                verb: CommandVerb::SET,
            }),
            BufferType::Command(Command {
                cmd: vec![
                    String::from("SET"),
                    String::from("bar"),
                    String::from("456"),
                ],
                verb: CommandVerb::SET,
            }),
            BufferType::Command(Command {
                cmd: vec![
                    String::from("SET"),
                    String::from("baz"),
                    String::from("789"),
                ],
                verb: CommandVerb::SET,
            }),
        ];
        assert_eq!(parse_buffer(&buffer), Some(expected_response));
    }
}
