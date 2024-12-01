use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::time::{Duration, Instant};

struct Item {
    value: String,
    saved_at: Instant,
    ttl: Option<usize>,
}

pub struct Store {
    store: HashMap<String, Item>,
}

impl Default for Store {
    fn default() -> Self {
        Store::new()
    }
}

impl Store {
    pub fn new() -> Store {
        Store {
            store: HashMap::new(),
        }
    }

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
                    _ = {
                        let (key, value) = parse_auxiliary_field(&mut content)?;
                        println!("Auxiliary field : {:?} = {:?}", key, value);
                    }
                }
                0xFE => {
                    let db_number = content.next()?;
                    println!("Selecting Database num: {db_number:?}");
                }
                0xFB => {
                    let hash_table_size = parse_length_encoded_int(&mut content)?;
                    let expire_hash_table_size = parse_length_encoded_int(&mut content)?;
                    let total_size = hash_table_size + expire_hash_table_size;
                    for _ in 0..total_size {
                        let (key, value) = parse_key_value(&mut content)?;
                        println!("{key:?}: {value:?}");
                        store.insert(
                            key,
                            Item {
                                value,
                                saved_at: Instant::now(),
                                ttl: None,
                            },
                        );
                    }
                }
                0xFE => {
                    let _ = parse_length_encoded_int(&mut content)?;
                }
                0xFF => {
                    // Just consume the last 8 bytes from the iterator
                    for _ in 0..8 {
                        content.next()?;
                    }
                }
                op_code => {
                    println!("Unexpected op_code: {}", op_code)
                }
            }
        }
        Some(Store { store })
    }

    pub fn set(&mut self, key: &str, value: &str, ttl: Option<usize>) {
        let item = Item {
            value: String::from(value),
            saved_at: Instant::now(),
            ttl,
        };
        self.store.insert(String::from(key), item);
    }

    pub fn get(&self, key: &str) -> Option<String> {
        let item = self.store.get(key)?;

        if let Some(ttl) = item.ttl {
            if item.saved_at.elapsed() > Duration::from_millis(ttl as u64) {
                return None;
            }
        }

        Some(item.value.clone())
    }

    pub fn get_keys(&self) -> Vec<String> {
        self.store.keys().map(|key| key.to_string()).collect()
    }
}

fn parse_magic_word<I>(content: &mut I) -> Option<String>
where
    I: Iterator<Item = u8>,
{
    let mut magic_word = [0; 5];
    for i in 0..5 {
        magic_word[i] = content.next()?;
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
    for i in 0..4 {
        version[i] = content.next()?;
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

fn parse_key_value<I>(content: &mut I) -> Option<(String, String)>
where
    I: Iterator<Item = u8>,
{
    let mut first_byte = content.next()?;
    if first_byte == 0xFD {
        // Just consume 4 bytes from iterator
        for _ in 0..4 {
            content.next()?;
        }
        first_byte = content.next()?;
    } else if first_byte == 0xFC {
        // Just consume 8 bytes from iterator
        for _ in 0..8 {
            content.next()?;
        }
        first_byte = content.next()?;
    };

    match first_byte {
        0 => {
            let (Some(Value::String(key)), Some(Value::String(value))) =
                (parse(content), parse(content))
            else {
                return None;
            };
            Some((key, value))
        }
        _ => panic!("Not implemented yet."),
    }
}

fn parse_u32<I>(content: &mut I) -> Option<u32>
where
    I: Iterator<Item = u8>,
{
    let mut values = [0u8; 4];
    for i in 0..4 {
        values[i] = content.next()?;
    }
    Some(u32::from_le_bytes(values))
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
            2 => parse_u32(content).map(|value| Value::Integer(value.into())),
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
    use core::time;
    use std::thread;

    use super::Store;

    #[test]
    fn set_and_get_value() {
        let mut store = Store::new();
        let key = String::from("toto");
        let value = String::from("tutu");

        store.set(&key, &value, None);

        assert_eq!(store.get(&key), Some(value));
    }

    #[test]
    fn set_with_ttl() {
        let mut store = Store::new();
        let key = String::from("toto");
        let value = String::from("tutu");

        store.set(&key, &value, Some(100));

        assert_eq!(store.get(&key), Some(value));

        thread::sleep(time::Duration::from_millis(100));

        assert_eq!(store.get(&key), None);
    }

    #[test]
    fn load_store_from_dbfile() {
        let dir = "./tests/assets";
        let dbname = "dump.rdb";

        let Some(store) = Store::from_dbfile(dir, dbname) else {
            panic!("Cannot load store from file");
        };

        assert_eq!(store.get("mykey"), Some(String::from("myval")));
    }
}
