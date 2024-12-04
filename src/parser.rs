use std::{
    str::{from_utf8, Utf8Error},
    usize,
};

const DELIMITER: &str = "\r\n";

#[derive(Debug, PartialEq, Eq)]
pub enum RESPSimpleType {
    Integer(i64),
    String(String),
    Error(String),
    Null,
    Array(Vec<RESPSimpleType>),
}

pub type Command = Vec<RESPSimpleType>;

pub fn parse_part(part: &str) -> RESPSimpleType {
    let (first_byte, data) = part.split_at(1);
    match first_byte {
        "+" => RESPSimpleType::String(data.to_owned()),
        "-" => RESPSimpleType::Error(data.to_owned()),
        ":" => RESPSimpleType::Integer(data.parse::<i64>().unwrap()),
        _ => panic!(),
    }
}

pub fn parse_command(bytes: &[u8]) -> Vec<Command> {
    let mut commands: Vec<Command> = Vec::new();
    let Some(data) = parse_bytes(bytes) else {
        return commands;
    };

    let mut iter = data.split(DELIMITER);
    while let Some(word) = iter.next() {
        if let Some(first_byte) = word.chars().next() {
            if first_byte != '*' {
                println!(
                    "Data does not correspond to an array, got {} as first byte instead of '*'.",
                    first_byte
                );
                // None
            } else {
                let command_len = word[1..].parse::<i32>().unwrap();
                commands.push(parse_array(command_len, &mut iter));
                // Some(parse_array(command_len, &mut iter))
            }
        } else {
            println!("Cannot parse because first word is empty.");
            // None
        }
    }
    commands
}

fn parse_bytes(bytes: &[u8]) -> Option<&str> {
    let data = match from_utf8(bytes) {
        Ok(data) => data,
        Err(err) => {
            let start = from_utf8(&bytes[0..err.valid_up_to()])
                .unwrap()
                .split("\r\n")
                .next()?;
            if start.starts_with("$") {
                let db_file_len = start[1..].parse::<usize>().ok()?;
                let db_file_header_len: usize = 9;
                println!(
                    "db file: {db_file_len} bytes // valid_up_to: {}",
                    err.valid_up_to()
                );
                return Some(
                    from_utf8(&bytes[err.valid_up_to() + db_file_len - db_file_header_len..])
                        .unwrap(),
                );
            }
            return None;
        }
    };
    Some(data)
}

fn parse_array<'a, I>(len: i32, iter: &mut I) -> Vec<RESPSimpleType>
where
    I: Iterator<Item = &'a str>,
{
    let mut elems: Vec<RESPSimpleType> = vec![];

    while let Some(word) = iter.next() {
        if word.is_empty() {
            break;
        }
        let (first_byte, data) = word.split_at(1);
        match first_byte {
            "+" => elems.push(RESPSimpleType::String(data.to_owned())),
            "-" => elems.push(RESPSimpleType::Error(data.to_owned())),
            ":" => elems.push(RESPSimpleType::Integer(data.parse::<i64>().unwrap())),
            "$" => elems.push(parse_bulk_string(data, iter)),
            byte => panic!("Unexpected byte value: {byte}"),
        }
        if elems.len() == len as usize {
            break;
        }
    }

    elems
}

pub fn parse_simple_type(bytes: &[u8]) -> Option<RESPSimpleType> {
    let data = from_utf8(bytes).ok()?;
    let mut iter = data.split(DELIMITER);

    let word = iter.next()?;

    if word.is_empty() {
        return None;
    }
    let (first_byte, data) = word.split_at(1);
    match first_byte {
        "+" => Some(RESPSimpleType::String(data.to_owned())),
        "-" => Some(RESPSimpleType::Error(data.to_owned())),
        ":" => Some(RESPSimpleType::Integer(data.parse::<i64>().unwrap())),
        "$" => Some(parse_bulk_string(data, &mut iter)),
        _ => panic!("Not implemented"),
    }
}

fn parse_bulk_string<'a, I>(data: &'a str, iter: &mut I) -> RESPSimpleType
where
    I: Iterator<Item = &'a str>,
{
    let str_len = data.parse::<i64>().unwrap();
    if str_len == -1 {
        return RESPSimpleType::Null;
    }
    if let Some(next_word) = iter.next() {
        RESPSimpleType::String(next_word.to_owned())
    } else {
        panic!();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_empty_command_none() {
        assert_eq!(parse_command(b""), Vec::<Command>::new());
    }

    #[test]
    fn test_parse_empty_first_word_none() {
        assert_eq!(parse_command(b"\r\n"), Vec::<Command>::new());
    }

    #[test]
    fn test_parse_not_an_array_none() {
        assert_eq!(parse_command(b"+OK\r\n"), Vec::<Command>::new());
    }

    #[test]
    fn test_parse_ping_command() {
        assert_eq!(
            parse_command(b"*1\r\n$4\r\nPING\r\n"),
            vec![vec![RESPSimpleType::String("PING".to_owned())]]
        );
    }

    #[test]
    fn test_parse_echo_command() {
        assert_eq!(
            parse_command(b"*2\r\n$4\r\nECHO\r\n$2\r\nOK\r\n"),
            vec![vec![
                RESPSimpleType::String("ECHO".to_owned()),
                RESPSimpleType::String("OK".to_owned())
            ]]
        );
    }
    #[test]
    fn test_parse_echo_command_null_bulk_string() {
        assert_eq!(
            parse_command(b"*2\r\n$4\r\nECHO\r\n$-1\r\n"),
            vec![vec![
                RESPSimpleType::String("ECHO".to_owned()),
                RESPSimpleType::Null
            ]]
        );
    }

    #[test]
    fn test_parse_echo_command_int() {
        assert_eq!(
            parse_command(b"*2\r\n$4\r\nECHO\r\n:-10\r\n"),
            vec![vec![
                RESPSimpleType::String("ECHO".to_owned()),
                RESPSimpleType::Integer(-10)
            ]]
        );

        assert_eq!(
            parse_command(b"*2\r\n$4\r\nECHO\r\n:0\r\n"),
            vec![vec![
                RESPSimpleType::String("ECHO".to_owned()),
                RESPSimpleType::Integer(0)
            ]]
        );

        assert_eq!(
            parse_command(b"*2\r\n$4\r\nECHO\r\n:10\r\n"),
            vec![vec![
                RESPSimpleType::String("ECHO".to_owned()),
                RESPSimpleType::Integer(10)
            ]]
        );
    }

    #[test]
    fn test_parse_multiple_commands() {
        let buffer = b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\n123\r\n*3\r\n$3\r\nSET\r\n$3\r\nbar\r\n$3\r\n456\r\n*3\r\n$3\r\nSET\r\n$3\r\nbaz\r\n$3\r\n789\r\n";
        assert_eq!(
            parse_command(buffer),
            vec![
                vec![
                    RESPSimpleType::String("SET".to_owned()),
                    RESPSimpleType::String("foo".to_owned()),
                    RESPSimpleType::String("123".to_owned()),
                ],
                vec![
                    RESPSimpleType::String("SET".to_owned()),
                    RESPSimpleType::String("bar".to_owned()),
                    RESPSimpleType::String("456".to_owned()),
                ],
                vec![
                    RESPSimpleType::String("SET".to_owned()),
                    RESPSimpleType::String("baz".to_owned()),
                    RESPSimpleType::String("789".to_owned()),
                ],
            ]
        )
    }
}
