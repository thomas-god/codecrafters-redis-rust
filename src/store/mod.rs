use core::fmt;
use std::{cmp::Ordering, collections::HashMap, error::Error};

use chrono::{DateTime, TimeDelta, Utc};

pub mod dbfile;

#[derive(Debug, PartialEq, Eq)]
pub struct StreamEntry {
    pub id: StreamEntryId,
    pub values: HashMap<String, String>,
}

#[derive(Debug, Clone)]
pub struct StreamEntryId {
    pub timestamp: usize,
    pub sequence_number: usize,
}

impl Ord for StreamEntryId {
    fn cmp(&self, other: &Self) -> Ordering {
        match (
            self.timestamp.cmp(&other.timestamp),
            self.sequence_number.cmp(&other.sequence_number),
        ) {
            (Ordering::Less, _) => Ordering::Less,
            (Ordering::Greater, _) => Ordering::Greater,
            (Ordering::Equal, Ordering::Less) => Ordering::Less,
            (Ordering::Equal, Ordering::Greater) => Ordering::Greater,
            (Ordering::Equal, Ordering::Equal) => Ordering::Equal,
        }
    }
}

impl PartialOrd for StreamEntryId {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for StreamEntryId {
    fn eq(&self, other: &Self) -> bool {
        self.timestamp == other.timestamp && self.sequence_number == other.sequence_number
    }
}

impl Eq for StreamEntryId {}

impl std::fmt::Display for StreamEntryId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}-{}", self.timestamp, self.sequence_number)
    }
}

#[derive(Debug, PartialEq)]
pub enum RequestedStreamEntryId {
    Explicit(StreamEntryId),
    AutoGenerateSequence(usize),
    AutoGenerate,
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

#[derive(Debug, PartialEq)]
pub enum AddStreamEntryError {
    EqualOrSmallerID,
    GreaterThanZeroZero,
}
impl Error for AddStreamEntryError {}

impl fmt::Display for AddStreamEntryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let message = match self {
            AddStreamEntryError::EqualOrSmallerID => {
                "ERR The ID specified in XADD is equal or smaller than the target stream top item"
            }
            AddStreamEntryError::GreaterThanZeroZero => {
                "ERR The ID specified in XADD must be greater than 0-0"
            }
        };
        write!(f, "{message}")
    }
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

    pub fn add_stream_entry(
        &mut self,
        key: &str,
        id_request: &RequestedStreamEntryId,
        entry: &HashMap<String, String>,
        ttl: Option<usize>,
    ) -> Result<String, AddStreamEntryError> {
        let expiry = ttl.and_then(|s| {
            Utc::now().checked_add_signed(TimeDelta::milliseconds(i64::try_from(s).ok()?))
        });

        match self.store.get_mut(key) {
            Some(Item {
                value: ItemType::Stream(existing_stream),
                expiry: _,
            }) => append_to_existing_stream(existing_stream, id_request, entry),
            _ => self.create_new_stream(key, id_request, entry, expiry),
        }
    }

    fn create_new_stream(
        &mut self,
        key: &str,
        id_request: &RequestedStreamEntryId,
        entry: &HashMap<String, String>,
        expiry: Option<DateTime<Utc>>,
    ) -> Result<String, AddStreamEntryError> {
        let id = match id_request {
            RequestedStreamEntryId::Explicit(id) => {
                if id
                    == (&StreamEntryId {
                        timestamp: 0,
                        sequence_number: 0,
                    })
                {
                    return Err(AddStreamEntryError::GreaterThanZeroZero);
                }
                id
            }
            RequestedStreamEntryId::AutoGenerateSequence(timestamp) => &StreamEntryId {
                timestamp: *timestamp,
                sequence_number: if *timestamp == 0 { 1 } else { 0 },
            },
            _ => todo!(),
        };
        let item = Item {
            value: ItemType::Stream(vec![StreamEntry {
                id: id.clone(),
                values: entry.clone(),
            }]),
            expiry,
        };
        self.store.insert(String::from(key), item);
        Ok(id.to_string())
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

fn append_to_existing_stream(
    existing_stream: &mut Vec<StreamEntry>,
    id_request: &RequestedStreamEntryId,
    entry: &HashMap<String, String>,
) -> Result<String, AddStreamEntryError> {
    let last_id = existing_stream
        .last()
        .map(|entry| &entry.id)
        .expect("Cannot be empty");

    let id = match id_request {
        RequestedStreamEntryId::Explicit(id) => {
            if id
                == (&StreamEntryId {
                    timestamp: 0,
                    sequence_number: 0,
                })
            {
                return Err(AddStreamEntryError::GreaterThanZeroZero);
            }
            if id <= last_id {
                return Err(AddStreamEntryError::EqualOrSmallerID);
            }
            id.clone()
        }
        RequestedStreamEntryId::AutoGenerateSequence(timestamp) => {
            let last_entry = existing_stream.last().expect("Cannot be empty");
            match timestamp.cmp(&last_entry.id.timestamp) {
                Ordering::Greater => StreamEntryId {
                    timestamp: *timestamp,
                    sequence_number: 0,
                },
                Ordering::Equal => StreamEntryId {
                    timestamp: *timestamp,
                    sequence_number: last_entry.id.sequence_number + 1,
                },
                Ordering::Less => return Err(AddStreamEntryError::EqualOrSmallerID),
            }
        }
        _ => todo!("not implemented yet"),
    };

    existing_stream.push(StreamEntry {
        id: id.clone(),
        values: entry.clone(),
    });
    Ok(id.to_string())
}

#[cfg(test)]
mod tests {
    use core::time;
    use std::{collections::HashMap, thread};

    use crate::store::{
        AddStreamEntryError, RequestedStreamEntryId, StoreType, StreamEntry, StreamEntryId,
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
    fn set_and_get_stream() {
        let mut store = Store::new();

        assert_eq!(store.get_stream(&String::from("toto")), None);

        let first_entry = HashMap::from([
            (String::from("temperature"), String::from("10")),
            (String::from("humidity"), String::from("80")),
        ]);
        let first_entry_id = StreamEntryId {
            timestamp: 0,
            sequence_number: 1,
        };

        assert_eq!(
            store.add_stream_entry(
                &String::from("toto"),
                &RequestedStreamEntryId::Explicit(first_entry_id.clone()),
                &first_entry,
                None
            ),
            Ok(String::from("0-1"))
        );

        assert_eq!(
            store.get_stream(&String::from("toto")),
            Some(&vec![StreamEntry {
                id: first_entry_id.clone(),
                values: first_entry.clone()
            }])
        );

        let second_entry = HashMap::from([
            (String::from("temperature"), String::from("12")),
            (String::from("humidity"), String::from("99")),
        ]);
        let second_entry_id = StreamEntryId {
            timestamp: 0,
            sequence_number: 2,
        };

        assert_eq!(
            store.add_stream_entry(
                &String::from("toto"),
                &RequestedStreamEntryId::Explicit(second_entry_id.clone()),
                &second_entry,
                None
            ),
            Ok(String::from("0-2"))
        );

        assert_eq!(
            store.get_stream(&String::from("toto")),
            Some(&vec![
                StreamEntry {
                    id: first_entry_id.clone(),
                    values: first_entry
                },
                StreamEntry {
                    id: second_entry_id,
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

        // Stream value
        let key = String::from("my-stream");
        let value = HashMap::from([
            (String::from("temperature"), String::from("10")),
            (String::from("humidity"), String::from("80")),
        ]);
        let entry_id = StreamEntryId {
            timestamp: 1,
            sequence_number: 0,
        };
        let _ = store.add_stream_entry(
            &key,
            &RequestedStreamEntryId::Explicit(entry_id.clone()),
            &value,
            Some(100),
        );

        if let Some(item_type) = store.get_type(&String::from("my-stream")) {
            assert_eq!(item_type, StoreType::Stream);
        } else {
            panic!("Should not be None but Some(StoreType::Stream)")
        }
    }

    #[test]
    fn test_set_stream_invalid_sequence_number() {
        let mut store = Store::new();

        // First insert
        let key = String::from("my-stream");
        let value = HashMap::from([
            (String::from("temperature"), String::from("10")),
            (String::from("humidity"), String::from("80")),
        ]);
        let entry_id = StreamEntryId {
            timestamp: 1,
            sequence_number: 1,
        };
        let _ = store.add_stream_entry(
            &key,
            &RequestedStreamEntryId::Explicit(entry_id.clone()),
            &value,
            Some(100),
        );

        // Another insert with same (timestamp, sequence_number)
        let res = store.add_stream_entry(
            &key,
            &RequestedStreamEntryId::Explicit(entry_id.clone()),
            &value,
            Some(100),
        );

        assert_eq!(res, Err(AddStreamEntryError::EqualOrSmallerID));
    }

    #[test]
    fn test_set_stream_greater_than_zero_zero() {
        let mut store = Store::new();

        let key = String::from("my-stream");
        let value = HashMap::from([
            (String::from("temperature"), String::from("10")),
            (String::from("humidity"), String::from("80")),
        ]);
        let entry_id = StreamEntryId {
            timestamp: 0,
            sequence_number: 0,
        };
        let res = store.add_stream_entry(
            &key,
            &RequestedStreamEntryId::Explicit(entry_id.clone()),
            &value,
            Some(100),
        );

        assert_eq!(res, Err(AddStreamEntryError::GreaterThanZeroZero));
    }
}
