use std::str::from_utf8;

const DELIMITER: &str = "\r\n";

#[derive(Debug, PartialEq, Eq)]
pub enum RESPSimpleType {
    Integer(i64),
    String(String),
    Error(String),
    Null,
    Array(Vec<RESPSimpleType>),
}

pub fn parse_part(part: &str) -> RESPSimpleType {
    let (first_byte, data) = part.split_at(1);
    match first_byte {
        "+" => RESPSimpleType::String(data.to_owned()),
        "-" => RESPSimpleType::Error(data.to_owned()),
        ":" => RESPSimpleType::Integer(data.parse::<i64>().unwrap()),
        _ => panic!(),
    }
}

pub fn parse_command(bytes: &[u8]) -> Option<Vec<RESPSimpleType>> {
    let data = from_utf8(bytes).unwrap();
    let mut iter = data.split(DELIMITER);
    let command_len: i32;
    if let Some(word) = iter.next() {
        if let Some(first_byte) = word.chars().next() {
            if first_byte != '*' {
                println!(
                    "Data does not correspond to an array, got {} as first byte instead of '*'.",
                    first_byte
                );
                None
            } else {
                command_len = word[1..].parse::<i32>().unwrap();
                Some(parse_array(command_len, &mut iter))
            }
        } else {
            println!("Cannot parse because first word is empty.");
            None
        }
    } else {
        println!("Cannot parse because of empty data.");
        None
    }
}

fn parse_array<'a, I>(_len: i32, iter: &mut I) -> Vec<RESPSimpleType>
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
            _ => panic!(),
        }
    }

    elems
}

pub fn parse_simple_type(bytes: &[u8]) -> Option<RESPSimpleType> {
    let data = from_utf8(bytes).unwrap();
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
        assert_eq!(parse_command(b""), None);
    }

    #[test]
    fn test_parse_empty_first_word_none() {
        assert_eq!(parse_command(b"\r\n"), None);
    }

    #[test]
    fn test_parse_not_an_array_none() {
        assert_eq!(parse_command(b"+OK\r\n"), None);
    }

    #[test]
    fn test_parse_ping_command() {
        assert_eq!(
            parse_command(b"*1\r\n$4\r\nPING\r\n"),
            Some(vec![RESPSimpleType::String("PING".to_owned())])
        );
    }

    #[test]
    fn test_parse_echo_command() {
        assert_eq!(
            parse_command(b"*2\r\n$4\r\nECHO\r\n$2\r\nOK\r\n"),
            Some(vec![
                RESPSimpleType::String("ECHO".to_owned()),
                RESPSimpleType::String("OK".to_owned())
            ])
        );
    }
    #[test]
    fn test_parse_echo_command_null_bulk_string() {
        assert_eq!(
            parse_command(b"*2\r\n$4\r\nECHO\r\n$-1\r\n"),
            Some(vec![
                RESPSimpleType::String("ECHO".to_owned()),
                RESPSimpleType::Null
            ])
        );
    }

    #[test]
    fn test_parse_echo_command_int() {
        assert_eq!(
            parse_command(b"*2\r\n$4\r\nECHO\r\n:-10\r\n"),
            Some(vec![
                RESPSimpleType::String("ECHO".to_owned()),
                RESPSimpleType::Integer(-10)
            ])
        );

        assert_eq!(
            parse_command(b"*2\r\n$4\r\nECHO\r\n:0\r\n"),
            Some(vec![
                RESPSimpleType::String("ECHO".to_owned()),
                RESPSimpleType::Integer(0)
            ])
        );

        assert_eq!(
            parse_command(b"*2\r\n$4\r\nECHO\r\n:10\r\n"),
            Some(vec![
                RESPSimpleType::String("ECHO".to_owned()),
                RESPSimpleType::Integer(10)
            ])
        );
    }
}
