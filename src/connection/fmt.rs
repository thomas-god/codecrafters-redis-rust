use crate::store::stream::{Stream, StreamEntry};

pub fn format_string(value: Option<String>) -> String {
    if let Some(value) = value {
        format!("${}\r\n{}\r\n", value.len(), value)
    } else {
        String::from("$-1\r\n")
    }
}

pub fn format_array(values: &Vec<String>) -> String {
    let mut response = String::new();

    response.push_str(format!("*{}\r\n", values.len()).as_str());

    for value in values {
        response.push_str(format_string(Some(value.clone())).as_str());
    }

    response
}

pub fn format_stream(stream: &Stream) -> String {
    let mut response = format!("*{}\r\n", stream.len());
    for entry in stream {
        response.push_str(&format_stream_entry(entry));
    }
    response
}

pub fn format_stream_entry(entry: &StreamEntry) -> String {
    let entry_id = format_string(Some(entry.id.to_string()));
    let entry_values: Vec<String> = entry
        .values
        .iter()
        .flat_map(|(k, v)| vec![[k.clone(), v.clone()]])
        .flatten()
        .collect();
    let entries = format_array(&entry_values);

    format!("*2\r\n{entry_id}{entries}")
}

#[cfg(test)]
mod tests {

    use indexmap::IndexMap;

    use crate::store::stream::StreamEntryId;

    use super::*;

    #[test]
    fn format_string_ok() {
        assert_eq!(
            String::from("$4\r\ntoto\r\n"),
            format_string(Some(String::from("toto")))
        );
        assert_eq!(
            String::from("$0\r\n\r\n"),
            format_string(Some(String::from("")))
        )
    }

    #[test]
    fn format_empty_string() {
        assert_eq!(String::from("$-1\r\n"), format_string(None))
    }

    #[test]
    fn test_format_empty_array() {
        assert_eq!(String::from("*0\r\n"), format_array(&Vec::new()));
    }

    #[test]
    fn test_format_array() {
        assert_eq!(
            String::from("*1\r\n$4\r\ntoto\r\n"),
            format_array(&vec![String::from("toto")])
        );
    }

    #[test]
    fn test_format_stream_entry() {
        let entry = StreamEntry {
            id: StreamEntryId {
                timestamp: 1526985054069,
                sequence_number: 0,
            },
            values: IndexMap::from([
                ("temperature".to_owned(), "36".to_owned()),
                ("humidity".to_owned(), "95".to_owned()),
            ]),
        };

        let expected = "*2\r\n$15\r\n1526985054069-0\r\n*4\r\n$11\r\ntemperature\r\n$2\r\n36\r\n$8\r\nhumidity\r\n$2\r\n95\r\n".to_owned();
        assert_eq!(format_stream_entry(&entry), expected);
    }

    #[test]
    fn test_format_stream() {
        let entry_1 = StreamEntry {
            id: StreamEntryId {
                timestamp: 1526985054069,
                sequence_number: 0,
            },
            values: IndexMap::from([
                ("temperature".to_owned(), "36".to_owned()),
                ("humidity".to_owned(), "95".to_owned()),
            ]),
        };
        let entry_2 = StreamEntry {
            id: StreamEntryId {
                timestamp: 1526985054079,
                sequence_number: 0,
            },
            values: IndexMap::from([
                ("temperature".to_owned(), "37".to_owned()),
                ("humidity".to_owned(), "94".to_owned()),
            ]),
        };
        let stream = vec![entry_1, entry_2];

        let expected = "*2\r\n*2\r\n$15\r\n1526985054069-0\r\n*4\r\n$11\r\ntemperature\r\n$2\r\n36\r\n$8\r\nhumidity\r\n$2\r\n95\r\n*2\r\n$15\r\n1526985054079-0\r\n*4\r\n$11\r\ntemperature\r\n$2\r\n37\r\n$8\r\nhumidity\r\n$2\r\n94\r\n".to_owned();

        assert_eq!(format_stream(&stream), expected);
    }
}
