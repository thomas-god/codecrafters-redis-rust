use std::str::from_utf8;

const DELIMITER: &str = "\r\n";

#[derive(Debug, PartialEq, Eq)]
pub enum RESPSimpleType<'a> {
    Integer(i64),
    String(&'a str),
    Error(&'a str),
    Null,
    Array(Vec<RESPSimpleType<'a>>),
}

pub fn parse_part(part: &str) -> RESPSimpleType {
    let (first_byte, data) = part.split_at(1);
    match first_byte {
        "+" => RESPSimpleType::String(data),
        "-" => RESPSimpleType::Error(data),
        ":" => RESPSimpleType::Integer(data.parse::<i64>().unwrap()),
        _ => panic!(),
    }
}

pub fn parse_command(bytes: &[u8]) -> Option<Vec<RESPSimpleType<'_>>> {
    let data = from_utf8(bytes).unwrap();
    let mut iter = data.split(DELIMITER);
    let command_len: i32;
    if let Some(word) = iter.next() {
        println!("parse_command word: {}", word);
        if let Some(first_byte) = word.chars().next() {
            if first_byte != '*' {
                println!(
                    "Data does not correspond to an array, got {} as first byte instead of '*'.",
                    first_byte
                );
                None
            } else {
                command_len = word[1..].parse::<i32>().unwrap();
                return Some(parse_array(command_len, &mut iter));
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

fn parse_array<'a, I>(_len: i32, iter: &mut I) -> Vec<RESPSimpleType<'a>>
where
    I: Iterator<Item = &'a str>,
{
    let mut elems: Vec<RESPSimpleType<'a>> = vec![];

    while let Some(word) = iter.next() {
        println!("parse_array word: {}", word);
        if word.is_empty() {
            break;
        }
        let (first_byte, data) = word.split_at(1);
        match first_byte {
            "+" => elems.push(RESPSimpleType::String(data)),
            "-" => elems.push(RESPSimpleType::Error(data)),
            ":" => elems.push(RESPSimpleType::Integer(data.parse::<i64>().unwrap())),
            "$" => elems.push(parse_bulk_string(data, iter)),
            _ => panic!(),
        }
    }

    elems
}

fn parse_bulk_string<'a, I>(data: &'a str, iter: &mut I) -> RESPSimpleType<'a>
where
    I: Iterator<Item = &'a str>,
{
    let str_len = data.parse::<i64>().unwrap();
    if str_len == -1 {
        return RESPSimpleType::Null;
    }
    if let Some(next_word) = iter.next() {
        println!("parse_bulk_string word: {}", next_word);
        return RESPSimpleType::String(next_word);
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
            Some(vec![RESPSimpleType::String("PING")])
        );
    }

    #[test]
    fn test_parse_echo_command() {
        assert_eq!(
            parse_command(b"*2\r\n$4\r\nECHO\r\n$2\r\nOK\r\n"),
            Some(vec![
                RESPSimpleType::String("ECHO"),
                RESPSimpleType::String("OK")
            ])
        );
    }
    #[test]
    fn test_parse_echo_command_null_bulk_string() {
        assert_eq!(
            parse_command(b"*2\r\n$4\r\nECHO\r\n$-1\r\n"),
            Some(vec![RESPSimpleType::String("ECHO"), RESPSimpleType::Null])
        );
    }

    #[test]
    fn test_parse_echo_command_int() {
        assert_eq!(
            parse_command(b"*2\r\n$4\r\nECHO\r\n:-10\r\n"),
            Some(vec![
                RESPSimpleType::String("ECHO"),
                RESPSimpleType::Integer(-10)
            ])
        );

        assert_eq!(
            parse_command(b"*2\r\n$4\r\nECHO\r\n:0\r\n"),
            Some(vec![
                RESPSimpleType::String("ECHO"),
                RESPSimpleType::Integer(0)
            ])
        );

        assert_eq!(
            parse_command(b"*2\r\n$4\r\nECHO\r\n:10\r\n"),
            Some(vec![
                RESPSimpleType::String("ECHO"),
                RESPSimpleType::Integer(10)
            ])
        );
    }
}
