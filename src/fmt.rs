pub fn format_string(value: Option<String>) -> String {
    if let Some(value) = value {
        format!("${}\r\n{}\r\n", value.len(), value)
    } else {
        String::from("$-1\r\n")
    }
}

pub fn format_array(values: Vec<String>) -> String {
    let mut response = String::new();

    response.push_str(format!("*{}\r\n", values.len()).as_str());

    for value in values {
        response.push_str(format_string(Some(value)).as_str());
    }

    response
}

#[cfg(test)]
mod tests {

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
        assert_eq!(String::from("*0\r\n"), format_array(Vec::new()));
    }

    #[test]
    fn test_format_array() {
        assert_eq!(
            String::from("*1\r\n$4\r\ntoto\r\n"),
            format_array(vec![String::from("toto")])
        );
    }
}
