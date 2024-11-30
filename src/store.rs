use std::collections::HashMap;
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
}
