use std::collections::HashMap;

use chrono::{DateTime, TimeDelta, Utc};

pub mod dbfile;

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
