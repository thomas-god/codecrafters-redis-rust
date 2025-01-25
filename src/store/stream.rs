use std::{cmp::Ordering, error::Error, fmt};

use chrono::{DateTime, TimeDelta, Utc};
use indexmap::IndexMap;

use super::{Item, Store, ValueType};

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct StreamEntry {
    pub id: StreamEntryId,
    pub values: IndexMap<String, String>,
}

#[derive(Debug, Clone, Copy)]
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

impl Store {
    pub fn add_stream_entry(
        &mut self,
        key: &str,
        id_request: &RequestedStreamEntryId,
        entry: &IndexMap<String, String>,
        ttl: Option<usize>,
    ) -> Result<StreamEntryId, AddStreamEntryError> {
        let expiry = ttl.and_then(|s| {
            Utc::now().checked_add_signed(TimeDelta::milliseconds(i64::try_from(s).ok()?))
        });

        match self.store.get_mut(key) {
            Some(Item {
                value: ValueType::Stream(existing_stream),
                expiry: _,
            }) => append_to_existing_stream(existing_stream, id_request, entry),
            _ => self.create_new_stream(key, id_request, entry, expiry),
        }
    }

    fn create_new_stream(
        &mut self,
        key: &str,
        id_request: &RequestedStreamEntryId,
        entry: &IndexMap<String, String>,
        expiry: Option<DateTime<Utc>>,
    ) -> Result<StreamEntryId, AddStreamEntryError> {
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
            RequestedStreamEntryId::AutoGenerate => {
                let now = chrono::Utc::now().timestamp_millis();
                &StreamEntryId {
                    timestamp: usize::try_from(now).unwrap_or(0),
                    sequence_number: if now == 0 { 1 } else { 0 },
                }
            }
        };
        let item = Item {
            value: ValueType::Stream(vec![StreamEntry {
                id: *id,
                values: entry.clone(),
            }]),
            expiry,
        };
        self.store.insert(String::from(key), item);
        Ok(id.to_owned())
    }

    pub fn get_stream_range(
        &self,
        key: &str,
        start: Option<&StreamEntryId>,
        end: Option<&StreamEntryId>,
    ) -> Vec<StreamEntry> {
        let Some(Item {
            value: ValueType::Stream(stream),
            expiry: _,
        }) = self.store.get(key)
        else {
            return Vec::new();
        };
        let matching_entries: Vec<StreamEntry> = stream
            .iter()
            .filter(|entry| {
                let start_condition = start.map(|start_id| entry.id >= *start_id).unwrap_or(true);
                let end_condition = end.map(|end_id| entry.id <= *end_id).unwrap_or(true);
                start_condition && end_condition
            })
            .map(|entry| StreamEntry {
                id: entry.id,
                values: entry.values.clone(),
            })
            .collect();
        matching_entries
    }

    #[cfg(test)]
    pub fn get_raw_stream(&self, key: &str) -> Option<&Stream> {
        let item = self.store.get(key)?;

        if let Some(expiry) = item.expiry {
            if expiry < Utc::now() {
                return None;
            }
        }

        let Item {
            value: ValueType::Stream(stream),
            expiry: _,
        } = item
        else {
            return None;
        };

        Some(stream)
    }
}

fn append_to_existing_stream(
    existing_stream: &mut Vec<StreamEntry>,
    id_request: &RequestedStreamEntryId,
    entry: &IndexMap<String, String>,
) -> Result<StreamEntryId, AddStreamEntryError> {
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
            *id
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
        RequestedStreamEntryId::AutoGenerate => {
            let now = usize::try_from(chrono::Utc::now().timestamp_millis()).unwrap_or(0);
            let last_entry = existing_stream.last().expect("Cannot be empty");
            match now.cmp(&last_entry.id.timestamp) {
                Ordering::Greater => StreamEntryId {
                    timestamp: now,
                    sequence_number: 0,
                },
                Ordering::Equal => StreamEntryId {
                    timestamp: now,
                    sequence_number: last_entry.id.sequence_number + 1,
                },
                Ordering::Less => StreamEntryId {
                    timestamp: last_entry.id.timestamp,
                    sequence_number: last_entry.id.sequence_number + 1,
                },
            }
        }
    };

    existing_stream.push(StreamEntry {
        id,
        values: entry.clone(),
    });
    Ok(id)
}

#[cfg(test)]
mod tests {
    use indexmap::IndexMap;

    use crate::store::{
        stream::{AddStreamEntryError, RequestedStreamEntryId, StreamEntry, StreamEntryId},
        Store,
    };

    #[test]
    fn set_and_get_stream() {
        let mut store = Store::new();

        assert_eq!(store.get_raw_stream(&String::from("toto")), None);

        let first_entry = IndexMap::from([
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
                &RequestedStreamEntryId::Explicit(first_entry_id),
                &first_entry,
                None
            ),
            Ok(StreamEntryId {
                timestamp: 0,
                sequence_number: 1
            })
        );

        assert_eq!(
            store.get_raw_stream(&String::from("toto")),
            Some(&vec![StreamEntry {
                id: first_entry_id,
                values: first_entry.clone()
            }])
        );

        let second_entry = IndexMap::from([
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
                &RequestedStreamEntryId::Explicit(second_entry_id),
                &second_entry,
                None
            ),
            Ok(StreamEntryId {
                timestamp: 0,
                sequence_number: 2
            })
        );

        assert_eq!(
            store.get_raw_stream(&String::from("toto")),
            Some(&vec![
                StreamEntry {
                    id: first_entry_id,
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
    fn test_set_stream_invalid_sequence_number() {
        let mut store = Store::new();

        // First insert
        let key = String::from("my-stream");
        let value = IndexMap::from([
            (String::from("temperature"), String::from("10")),
            (String::from("humidity"), String::from("80")),
        ]);
        let entry_id = StreamEntryId {
            timestamp: 1,
            sequence_number: 1,
        };
        let _ = store.add_stream_entry(
            &key,
            &RequestedStreamEntryId::Explicit(entry_id),
            &value,
            Some(100),
        );

        // Another insert with same (timestamp, sequence_number)
        let res = store.add_stream_entry(
            &key,
            &RequestedStreamEntryId::Explicit(entry_id),
            &value,
            Some(100),
        );

        assert_eq!(res, Err(AddStreamEntryError::EqualOrSmallerID));
    }

    #[test]
    fn test_set_stream_greater_than_zero_zero() {
        let mut store = Store::new();

        let key = String::from("my-stream");
        let value = IndexMap::from([
            (String::from("temperature"), String::from("10")),
            (String::from("humidity"), String::from("80")),
        ]);
        let entry_id = StreamEntryId {
            timestamp: 0,
            sequence_number: 0,
        };
        let res = store.add_stream_entry(
            &key,
            &RequestedStreamEntryId::Explicit(entry_id),
            &value,
            Some(100),
        );

        assert_eq!(res, Err(AddStreamEntryError::GreaterThanZeroZero));
    }

    #[test]
    fn get_empty_range() {
        let store = Store::new();

        assert_eq!(store.get_stream_range("my-key", None, None), Vec::new());
    }
}
