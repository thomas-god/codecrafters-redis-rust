use std::str::from_utf8;

use itertools::Itertools;

#[derive(Debug, PartialEq)]
pub enum BufferType {
    String(String),
    DBFile(Vec<u8>),
    Array(Vec<String>),
}

#[derive(Debug, PartialEq)]
pub struct BufferElement {
    pub value: BufferType,
    pub n_bytes: NumberOfBytesParsed,
}

type NumberOfBytesParsed = usize;

const OP_CODE_LEN: usize = 1;
const DELIMITER_LEN: usize = 2;

pub fn parse_buffer(buffer: &[u8]) -> Option<Vec<BufferElement>> {
    let mut buffer_iter = buffer.iter();

    let mut elements: Vec<BufferElement> = Vec::new();
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
                if let Some(array) = parse_array(&mut buffer_iter) {
                    elements.push(array);
                }
            }
            _ => panic!(),
        }
    }
    Some(elements)
}

fn parse_simple_string(iterator: &mut std::slice::Iter<'_, u8>) -> Option<BufferElement> {
    let bytes = find_until_next_delimiter(iterator);
    from_utf8(&bytes)
        .map(|word| BufferElement {
            value: BufferType::String(word.to_string()),
            n_bytes: OP_CODE_LEN + bytes.len() + DELIMITER_LEN, // '+' and '\r\n'
        })
        .ok()
}

fn parse_bulk_string_like(iterator: &mut std::slice::Iter<'_, u8>) -> Option<BufferElement> {
    let len = from_utf8(&find_until_next_delimiter(iterator))
        .ok()
        .and_then(|bytes| bytes.parse::<usize>().ok())?;
    let mut bytes: Vec<u8> = Vec::new();
    for _ in 0..len {
        let _ = iterator.next().map(|byte| bytes.push(*byte));
    }
    let base_n_bytes = OP_CODE_LEN // First '$'
    + len.to_string().as_bytes().len() // Number of bytes for the len part
    + DELIMITER_LEN // '\r\n' separator
    + len; // string's actual number of bytes;

    // Check if the next 2 bytes are a delimiter (it's a bulk string, consume those 2 bytes) or not
    // (it's a DB file, do not consume those 2 bytes)
    let mut iter_peek = iterator.clone().tuple_windows::<(_, _)>().peekable();
    match iter_peek.peek() {
        Some((first, second)) if (**first, **second) == (b'\r', b'\n') => {
            iterator.next();
            iterator.next();
            return from_utf8(&bytes)
                .ok()
                .map(|word| BufferType::String(word.to_string()))
                .map(|value| BufferElement {
                    value,
                    n_bytes: base_n_bytes + DELIMITER_LEN,
                });
        }
        _ => Some(BufferElement {
            value: BufferType::DBFile(bytes),
            n_bytes: base_n_bytes,
        }),
    }
}

fn parse_array(iterator: &mut std::slice::Iter<'_, u8>) -> Option<BufferElement> {
    let mut bytes_processed = OP_CODE_LEN; // Initial '*' character

    let len = from_utf8(&find_until_next_delimiter(iterator))
        .ok()
        .and_then(|bytes| bytes.parse::<usize>().ok())?;
    bytes_processed += len.to_string().bytes().len() + DELIMITER_LEN; // Number of bytes to represent the length + '\r\n'

    let mut elements: Vec<String> = Vec::new();
    for _ in 0..len {
        iterator.next();
        if let Some(BufferElement {
            value: BufferType::String(elem),
            n_bytes,
        }) = parse_bulk_string_like(iterator)
        {
            elements.push(elem);
            bytes_processed += n_bytes;
        }
    }

    Some(BufferElement {
        value: BufferType::Array(elements),
        n_bytes: bytes_processed,
    })
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
    use super::{parse_buffer, BufferElement, BufferType};

    #[test]
    fn buffer_with_simple_string() {
        let buffer = String::from("+OK\r\n").into_bytes();
        let expected_response = vec![BufferElement {
            value: BufferType::String(String::from("OK")),
            n_bytes: 5,
        }];
        assert_eq!(parse_buffer(&buffer), Some(expected_response));
    }

    #[test]
    fn buffer_with_2_simple_strings() {
        let buffer = String::from("+OK\r\n+hello\r\n").into_bytes();
        let expected_response = vec![
            BufferElement {
                value: BufferType::String(String::from("OK")),
                n_bytes: 5,
            },
            BufferElement {
                value: BufferType::String(String::from("hello")),
                n_bytes: 8,
            },
        ];
        assert_eq!(parse_buffer(&buffer), Some(expected_response));
    }

    #[test]
    fn buffer_with_bulk_string() {
        let buffer = String::from("$4\r\nPING\r\n").into_bytes();
        let expected_response = vec![BufferElement {
            value: BufferType::String(String::from("PING")),
            n_bytes: 10,
        }];
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
        let expected_response = vec![BufferElement {
            value: BufferType::DBFile(db_file),
            n_bytes: 93,
        }];
        assert_eq!(parse_buffer(&buffer), Some(expected_response));
    }

    #[test]
    fn buffer_with_2_bulk_strings() {
        let buffer = String::from("$4\r\nPING\r\n$4\r\nPONG\r\n").into_bytes();
        let expected_response = vec![
            BufferElement {
                value: BufferType::String(String::from("PING")),
                n_bytes: 10,
            },
            BufferElement {
                value: BufferType::String(String::from("PONG")),
                n_bytes: 10,
            },
        ];
        assert_eq!(parse_buffer(&buffer), Some(expected_response));
    }

    #[test]
    fn test_buffer_with_array() {
        let buffer = String::from("*2\r\n$4\r\nECHO\r\n$4\r\ntoto\r\n").into_bytes();
        let expected_response = vec![BufferElement {
            value: BufferType::Array(vec![String::from("ECHO"), String::from("toto")]),
            n_bytes: 24,
        }];
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
            BufferElement {
                value: BufferType::String(String::from(
                    "FULLRESYNC 75cd7bc10c49047e0d163660f3b90625b1af31dc 0",
                )),
                n_bytes: 56,
            },
            BufferElement {
                value: BufferType::DBFile(db_file),
                n_bytes: 93,
            },
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
            BufferElement {
                value: BufferType::String(String::from(
                    "FULLRESYNC 75cd7bc10c49047e0d163660f3b90625b1af31dc 0",
                )),
                n_bytes: 56,
            },
            BufferElement {
                value: BufferType::DBFile(db_file),
                n_bytes: 93,
            },
            BufferElement {
                value: BufferType::Array(vec![
                    String::from("SET"),
                    String::from("foo"),
                    String::from("123"),
                ]),
                n_bytes: 31,
            },
            BufferElement {
                value: BufferType::Array(vec![
                    String::from("SET"),
                    String::from("bar"),
                    String::from("456"),
                ]),
                n_bytes: 31,
            },
            BufferElement {
                value: BufferType::Array(vec![
                    String::from("SET"),
                    String::from("baz"),
                    String::from("789"),
                ]),
                n_bytes: 31,
            },
        ];
        assert_eq!(parse_buffer(&buffer), Some(expected_response));
    }
}
