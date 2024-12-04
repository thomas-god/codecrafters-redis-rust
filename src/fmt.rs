use crate::parser::RESPSimpleType;

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

pub fn format_command(command: &[RESPSimpleType]) -> String {
    let mut message = String::new();

    message.push_str(format!("*{}\r\n", command.len()).as_str());
    for word in command {
        match word {
            RESPSimpleType::String(string) => {
                message.push_str(format_string(Some(string.clone())).as_str())
            }
            RESPSimpleType::Integer(number) => {
                message.push_str(format!(":{}\r\n", number).as_str())
            }
            _ => panic!("Format not implemented yet"),
        }
    }
    message
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

    #[test]
    fn test_format_command() {
        assert_eq!(
            String::from("*5\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n$2\r\npx\r\n:10\r\n"),
            format_command(&[
                RESPSimpleType::String(String::from("SET")),
                RESPSimpleType::String(String::from("foo")),
                RESPSimpleType::String(String::from("bar")),
                RESPSimpleType::String(String::from("px")),
                RESPSimpleType::Integer(10)
            ])
        );
    }
}
