use std::collections::HashMap;
use std::fs;
use std::path::Path;

use chrono::{DateTime, TimeDelta, Utc};

#[derive(Debug, PartialEq, Eq)]
pub struct StreamEntry {
    pub id: String,
    pub values: HashMap<String, String>,
}
pub type Stream = Vec<StreamEntry>;

#[derive(Debug, PartialEq, Eq)]
enum ItemType {
    String(String),
    Stream(Stream),
}

#[derive(Debug, PartialEq, Eq)]
pub enum StoreType {
    String,
    Stream,
}
struct Item {
    value: ItemType,
    expiry: Option<DateTime<Utc>>,
}

pub struct Store {
    store: HashMap<String, Item>,
    pub n_replicas: u64,
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
            n_replicas: 0,
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
                                value: ItemType::String(value),
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

    pub fn set_string(&mut self, key: &str, value: &str, ttl: Option<usize>) {
        let expiry = ttl.and_then(|s| {
            Utc::now().checked_add_signed(TimeDelta::milliseconds(i64::try_from(s).ok()?))
        });
        let item = Item {
            value: ItemType::String(String::from(value)),
            expiry,
        };
        self.store.insert(String::from(key), item);
    }

    pub fn get_string(&self, key: &str) -> Option<String> {
        let item = self.store.get(key)?;

        if let Some(expiry) = item.expiry {
            if expiry < Utc::now() {
                return None;
            }
        }

        let Item {
            value: ItemType::String(value),
            expiry: _,
        } = item
        else {
            return None;
        };

        Some(value.clone())
    }

    pub fn set_stream(
        &mut self,
        key: &str,
        id: &str,
        entry: &HashMap<String, String>,
        ttl: Option<usize>,
    ) -> Option<String> {
        let expiry = ttl.and_then(|s| {
            Utc::now().checked_add_signed(TimeDelta::milliseconds(i64::try_from(s).ok()?))
        });
        let Some(Item {
            value: ItemType::Stream(existing_stream),
            expiry: _,
        }) = self.store.get_mut(key)
        else {
            let item = Item {
                value: ItemType::Stream(vec![StreamEntry {
                    id: id.to_string(),
                    values: entry.clone(),
                }]),
                expiry,
            };
            self.store.insert(String::from(key), item);
            return Some(id.to_string());
        };

        existing_stream.push(StreamEntry {
            id: id.to_string(),
            values: entry.clone(),
        });
        Some(id.to_string())
    }

    pub fn get_stream(&self, key: &str) -> Option<&Stream> {
        let item = self.store.get(key)?;

        if let Some(expiry) = item.expiry {
            if expiry < Utc::now() {
                return None;
            }
        }

        let Item {
            value: ItemType::Stream(stream),
            expiry: _,
        } = item
        else {
            return None;
        };

        Some(stream)
    }

    pub fn get_keys(&self) -> Vec<String> {
        self.store.keys().map(|key| key.to_string()).collect()
    }

    pub fn get_type(&self, key: &str) -> Option<StoreType> {
        let item = self.store.get(key)?;
        Some(match item.value {
            ItemType::Stream(_) => StoreType::Stream,
            ItemType::String(_) => StoreType::String,
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
    use core::time;
    use std::{collections::HashMap, thread};

    use crate::store::{StoreType, StreamEntry};

    use super::Store;

    #[test]
    fn set_and_get_string_value() {
        let mut store = Store::new();
        let key = String::from("toto");
        let value = String::from("tutu");

        store.set_string(&key, &value, None);

        assert_eq!(store.get_string(&key), Some(value));
    }

    #[test]
    fn set_with_ttl() {
        let mut store = Store::new();
        let key = String::from("toto");
        let value = String::from("tutu");

        store.set_string(&key, &value, Some(100));

        assert_eq!(store.get_string(&key), Some(value));

        thread::sleep(time::Duration::from_millis(100));

        assert_eq!(store.get_string(&key), None);
    }

    #[test]
    fn load_store_from_dbfile() {
        let dir = "./tests/assets";
        let dbname = "dump.rdb";

        let Some(store) = Store::from_dbfile(dir, dbname) else {
            panic!("Cannot load store from file");
        };

        assert_eq!(store.get_string("mykey"), Some(String::from("myval")));
    }

    #[test]
    fn set_and_get_stream() {
        let mut store = Store::new();

        assert_eq!(store.get_stream(&String::from("toto")), None);

        let first_entry = HashMap::from([
            (String::from("temperature"), String::from("10")),
            (String::from("humidity"), String::from("80")),
        ]);

        assert_eq!(
            store.set_stream(
                &String::from("toto"),
                &String::from("0-1"),
                &first_entry,
                None
            ),
            Some(String::from("0-1"))
        );

        assert_eq!(
            store.get_stream(&String::from("toto")),
            Some(&vec![StreamEntry {
                id: String::from("0-1"),
                values: first_entry.clone()
            }])
        );

        let second_entry = HashMap::from([
            (String::from("temperature"), String::from("12")),
            (String::from("humidity"), String::from("99")),
        ]);

        assert_eq!(
            store.set_stream(
                &String::from("toto"),
                &String::from("0-2"),
                &second_entry,
                None
            ),
            Some(String::from("0-2"))
        );

        assert_eq!(
            store.get_stream(&String::from("toto")),
            Some(&vec![
                StreamEntry {
                    id: String::from("0-1"),
                    values: first_entry
                },
                StreamEntry {
                    id: String::from("0-2"),
                    values: second_entry
                }
            ])
        );
    }

    #[test]
    fn test_get_type() {
        let mut store = Store::new();

        // No value for this key
        assert_eq!(store.get_type(&String::from("no-key")), None);

        // String value
        let key = String::from("my-string");
        let value = String::from("tutu");
        store.set_string(&key, &value, Some(100));

        if let Some(item_type) = store.get_type(&String::from("my-string")) {
            assert_eq!(item_type, StoreType::String);
        } else {
            panic!("Should not be None but Some(StoreType::String)")
        }

        // Stram value
        let key = String::from("my-stream");
        let value = HashMap::from([
            (String::from("temperature"), String::from("10")),
            (String::from("humidity"), String::from("80")),
        ]);
        store.set_stream(&key, &String::from("id"), &value, Some(100));

        if let Some(item_type) = store.get_type(&String::from("my-stream")) {
            assert_eq!(item_type, StoreType::Stream);
        } else {
            panic!("Should not be None but Some(StoreType::Stream)")
        }
    }
}
