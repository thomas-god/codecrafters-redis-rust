use std::{collections::HashMap, fs, path::Path};

use chrono::{DateTime, Utc};

use crate::store::{Item, ValueType};

use super::Store;

impl Store {
    pub fn from_dbfile(dir: &str, dbname: &str) -> Option<Store> {
        let path = Path::new(dir).join(dbname);
        let mut content = fs::read(path).ok()?.into_iter();

        let magic_word = parse_magic_word(&mut content)?;
        let version = parse_version(&mut content)?;
        println!("magic word: {magic_word}");
        println!("version: {version}");

        let mut store: HashMap<String, Item> = HashMap::new();

        while let Some(op_code) = content.next() {
            match op_code {
                0xFA => {
                    let (key, value) = parse_auxiliary_field(&mut content)?;
                    println!("Auxiliary field : {:?} = {:?}", key, value);
                }
                0xFE => {
                    let db_number = content.next()?;
                    println!("Selecting Database num: {db_number:?}");
                }
                0xFB => {
                    let hash_table_size = parse_length_encoded_int(&mut content)?;
                    let expire_hash_table_size = parse_length_encoded_int(&mut content)?;
                    println!("Total keys to load: {hash_table_size}");
                    println!("Including {expire_hash_table_size} keys with expiry");
                    for _ in 0..hash_table_size {
                        let (key, value, expiry) = parse_key_value(&mut content)?;
                        println!("{key:?}: {value:?} (expired at {expiry:?})");
                        store.insert(
                            key,
                            Item {
                                value: ValueType::String(value),
                                expiry,
                            },
                        );
                    }
                }
                0xFF => {
                    // Just consume the last 8 bytes from the iterator (checksum bytes, not checked
                    // for now).
                    for _ in 0..8 {
                        content.next()?;
                    }
                }
                op_code => {
                    println!("Unexpected op_code: {}", op_code)
                }
            }
        }
        Some(Store {
            store,
            n_replicas: 0,
        })
    }
}

fn parse_magic_word<I>(content: &mut I) -> Option<String>
where
    I: Iterator<Item = u8>,
{
    let mut magic_word = [0; 5];
    for w in &mut magic_word {
        *w = content.next()?;
    }
    if magic_word != [0x52, 0x45, 0x44, 0x49, 0x53] {
        return None;
    }
    String::from_utf8(magic_word.to_vec()).ok()
}

fn parse_version<I>(content: &mut I) -> Option<String>
where
    I: Iterator<Item = u8>,
{
    let mut version = [0; 4];
    for v in &mut version {
        *v = content.next()?;
    }
    String::from_utf8(version.to_vec()).ok()
}

fn parse_auxiliary_field<I>(content: &mut I) -> Option<(Value, Value)>
where
    I: Iterator<Item = u8>,
{
    let key = parse(content)?;
    let value = parse(content)?;
    Some((key, value))
}

fn parse_u8<I>(content: &mut I) -> Option<u8>
where
    I: Iterator<Item = u8>,
{
    content.next()
}

fn parse_key_value<I>(content: &mut I) -> Option<(String, String, Option<DateTime<Utc>>)>
where
    I: Iterator<Item = u8>,
{
    let mut first_byte = content.next()?;
    let mut expiry: Option<DateTime<Utc>> = None;
    if first_byte == 0xFD {
        expiry = parse_u32(content).map(|epoch| DateTime::from_timestamp(epoch.into(), 0))?;
        first_byte = content.next()?;
    } else if first_byte == 0xFC {
        expiry = parse_u64(content)
            .map(|epoch| DateTime::from_timestamp_millis(i64::try_from(epoch).ok()?))?;
        first_byte = content.next()?;
    };

    match first_byte {
        0 => {
            let (Some(Value::String(key)), Some(Value::String(value))) =
                (parse(content), parse(content))
            else {
                return None;
            };
            Some((key, value, expiry))
        }
        first_byte => panic!("Not implemented yet for first_byte={}.", first_byte),
    }
}

fn parse_u32<I>(content: &mut I) -> Option<u32>
where
    I: Iterator<Item = u8>,
{
    let mut values = [0u8; 4];
    for value in &mut values {
        *value = content.next()?;
    }
    Some(u32::from_le_bytes(values))
}

fn parse_u64<I>(content: &mut I) -> Option<u64>
where
    I: Iterator<Item = u8>,
{
    let mut values = [0u8; 8];
    for value in &mut values {
        *value = content.next()?;
    }
    Some(u64::from_le_bytes(values))
}

#[derive(Debug, PartialEq, Eq)]

enum Value {
    String(String),
    Integer(u32),
}

fn parse<I>(content: &mut I) -> Option<Value>
where
    I: Iterator<Item = u8>,
{
    let length_byte = content.next()?;
    match length_byte & 0b11000000 {
        0b00000000 => {
            let length_to_parse = length_byte & 0b00111111;
            let mut value: Vec<u8> = Vec::new();
            for _ in 0..length_to_parse {
                value.push(content.next()?);
            }
            Some(Value::String(String::from_utf8(value).ok()?))
        }
        0b01000000 => panic!("Not implemented"),
        0b10000000 => panic!("Not implemented"),
        0b11000000 => match length_byte & 0b00111111 {
            0 => parse_u8(content).map(|value| Value::Integer(value.into())),
            1 => panic!("Not implemented"),
            2 => parse_u32(content).map(Value::Integer),
            n => panic!("Unknown integer to parse, expected 0, 1 or 2 but got {n}"),
        },
        _ => panic!("Wrong bits format for length encoding."),
    }
}

fn parse_length_encoded_int<I>(content: &mut I) -> Option<u32>
where
    I: Iterator<Item = u8>,
{
    let length_byte = content.next()?;
    match length_byte & 0b11000000 {
        0b00000000 => Some((length_byte & 0b00111111).into()),
        0b01000000 => panic!("Not implemented"),
        0b10000000 => panic!("Not implemented"),
        0b11000000 => match length_byte & 0b00111111 {
            0 => parse_u8(content).map(|value| value.into()),
            1 => panic!("Not implemented"),
            2 => parse_u32(content),
            n => panic!("Unknown integer to parse, expected 0, 1 or 2 but got {n}"),
        },
        _ => panic!("Wrong bits format for length encoding."),
    }
}

#[cfg(test)]
mod tests {
    use crate::store::Store;

    #[test]
    fn load_store_from_dbfile() {
        let dir = "./tests/assets";
        let dbname = "dump.rdb";

        let Some(store) = Store::from_dbfile(dir, dbname) else {
            panic!("Cannot load store from file");
        };

        assert_eq!(store.get_string("mykey"), Some(String::from("myval")));
    }
}
