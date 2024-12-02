pub fn format_string(value: Option<String>) -> String {
    if let Some(value) = value {
        format!("${}\r\n{}\r\n", value.len(), value)
    } else {
        String::from("$-1\r\n")
    }
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
}
