use std::collections::HashMap;

use chrono::{DateTime, TimeDelta, Utc};
use stream::Stream;

pub mod dbfile;
pub mod stream;

struct Item {
    value: ValueType,
    expiry: Option<DateTime<Utc>>,
}

#[derive(Debug, PartialEq, Eq)]
enum ValueType {
    String(String),
    Stream(Stream),
}

#[derive(Debug, PartialEq, Eq)]
pub enum ItemType {
    String,
    Stream,
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

    pub fn set_string(&mut self, key: &str, value: &str, ttl: Option<usize>) {
        let expiry = ttl.and_then(|s| {
            Utc::now().checked_add_signed(TimeDelta::milliseconds(i64::try_from(s).ok()?))
        });
        let item = Item {
            value: ValueType::String(String::from(value)),
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
            value: ValueType::String(value),
            expiry: _,
        } = item
        else {
            return None;
        };

        Some(value.clone())
    }

    pub fn incr(&mut self, key: &str) -> Option<usize> {
        match self.store.get(key) {
            Some(Item {
                value: ValueType::String(val),
                expiry,
            }) => {
                let vaaaalue = val.clone();
                let new_val = vaaaalue.parse::<usize>().unwrap() + 1;
                self.store.insert(
                    key.to_owned(),
                    Item {
                        value: ValueType::String(new_val.to_string()),
                        expiry: *expiry,
                    },
                );
                Some(new_val)
            }
            _ => {
                self.store.insert(
                    key.to_owned(),
                    Item {
                        value: ValueType::String(1.to_string()),
                        expiry: None,
                    },
                );
                Some(1)
            }
        }
    }

    pub fn get_keys(&self) -> Vec<String> {
        self.store.keys().map(|key| key.to_string()).collect()
    }

    pub fn get_item_type(&self, key: &str) -> Option<ItemType> {
        let item = self.store.get(key)?;
        Some(match item.value {
            ValueType::Stream(_) => ItemType::Stream,
            ValueType::String(_) => ItemType::String,
        })
    }
}

#[cfg(test)]
mod tests {
    use core::time;
    use std::thread;

    use indexmap::IndexMap;

    use crate::store::{
        stream::{RequestedStreamEntryId, StreamEntryId},
        ItemType,
    };

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
    fn test_get_type() {
        let mut store = Store::new();

        // No value for this key
        assert_eq!(store.get_item_type(&String::from("no-key")), None);

        // String value
        let key = String::from("my-string");
        let value = String::from("tutu");
        store.set_string(&key, &value, Some(100));

        if let Some(item_type) = store.get_item_type(&String::from("my-string")) {
            assert_eq!(item_type, ItemType::String);
        } else {
            panic!("Should not be None but Some(StoreType::String)")
        }

        // Stream value
        let key = String::from("my-stream");
        let value = IndexMap::from([
            (String::from("temperature"), String::from("10")),
            (String::from("humidity"), String::from("80")),
        ]);
        let entry_id = StreamEntryId {
            timestamp: 1,
            sequence_number: 0,
        };
        let _ = store.add_stream_entry(
            &key,
            &RequestedStreamEntryId::Explicit(entry_id),
            &value,
            Some(100),
        );

        if let Some(item_type) = store.get_item_type(&String::from("my-stream")) {
            assert_eq!(item_type, ItemType::Stream);
        } else {
            panic!("Should not be None but Some(StoreType::Stream)")
        }
    }
}
