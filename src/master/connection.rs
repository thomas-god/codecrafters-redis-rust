use std::{cell::Cell, fmt::Debug, fs, iter::zip, net::TcpStream};

use indexmap::IndexMap;
use itertools::Itertools;

use crate::{
    config::{Config, ReplicationRole},
    connections::{
        fmt::{format_array, format_stream, format_string},
        parser::{BufferType, Command, CommandVerb},
        stream::RedisStream,
        PollResult, ReplicationCheckRequest, WriteToStream,
    },
    store::{
        stream::{RequestedStreamEntryId, StreamEntry, StreamEntryId},
        ItemType, Store,
    },
};

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum ConnectionRole {
    Client,
    Replica,
}

pub struct MasterToClientConnection {
    stream: RedisStream<TcpStream>,
    pub active: bool,
    pub connected_with: ConnectionRole,
    wait_for_replication_ack: bool,
    replication: Option<Replication>,
}

#[derive(Debug)]
struct Replication {
    replication_offset: usize,
    last_offset_checked: usize,
}

impl Replication {
    fn match_offsets(&mut self) {
        self.last_offset_checked = self.replication_offset;
    }

    fn need_to_ask_for_replication_ack(&self) -> bool {
        println!(
            "current offset: {}, last offset {}",
            self.replication_offset, self.last_offset_checked
        );
        self.last_offset_checked < self.replication_offset
    }
}

impl MasterToClientConnection {
    pub fn new(stream: TcpStream) -> MasterToClientConnection {
        stream
            .set_nonblocking(true)
            .expect("Cannot put TCP stream in non-blocking mode");
        let stream = RedisStream::new(stream);
        MasterToClientConnection {
            stream,
            active: true,
            wait_for_replication_ack: false,
            connected_with: ConnectionRole::Client,
            replication: None,
        }
    }

    pub fn poll(&mut self, global_state: &mut Cell<Store>, config: &Config) -> Vec<PollResult> {
        match self.stream.read() {
            Some(elements) => {
                let mut poll_results: Vec<PollResult> = vec![];
                for element in elements {
                    match element {
                        BufferType::Command(cmd) => {
                            if let Some(result) = self.process_command(&cmd, global_state, config) {
                                poll_results.push(result);
                            }
                        }
                        elem => println!("Nothing to do for: {elem:?}"),
                    }
                }
                if poll_results
                    .iter()
                    .any(|r| *r == PollResult::PromoteToReplica)
                {
                    println!(
                        "Promoting connection to replica role, will propagate future writes to it."
                    );
                    self.connected_with = ConnectionRole::Replica;
                }
                poll_results
            }
            _ => {
                println!("Unable to read from stream");
                self.active = false;
                vec![]
            }
        }
    }

    pub fn send_command(&mut self, command: &Vec<String>) {
        let message = format_array(command);
        self.send_string(&message);
        self.stream.read();
    }

    pub fn send_string(&mut self, message: &str) {
        self.stream.send_string(message);
    }

    pub fn notify_replication_ack(&mut self, n_replicas: usize) {
        if !self.wait_for_replication_ack {
            return;
        }
        println!("Notifying client of replication ack");
        self.send_string(&format!(":{}\r\n", n_replicas));
        self.wait_for_replication_ack = false;
    }

    fn process_command(
        &mut self,
        cmd: &Command,
        global_state: &mut Cell<Store>,
        config: &Config,
    ) -> Option<PollResult> {
        println!("Processing command: {cmd:?}");
        let Command { verb, cmd } = cmd;
        match verb {
            CommandVerb::PING => self.process_ping(),
            CommandVerb::ECHO => self.process_echo(cmd),
            CommandVerb::SET => self.process_set(cmd, global_state),
            CommandVerb::GET => self.process_get(cmd, global_state),
            CommandVerb::TYPE => self.process_type(cmd, global_state),
            CommandVerb::XADD => self.process_xadd(cmd, global_state),
            CommandVerb::XRANGE => self.process_xrange(cmd, global_state),
            CommandVerb::XREAD => self.process_xread(cmd, global_state),
            CommandVerb::CONFIG => self.process_config(cmd, config),
            CommandVerb::KEYS => self.process_keys(cmd, global_state),
            CommandVerb::INFO => self.process_info(cmd, config),
            CommandVerb::REPLCONF => self.process_replconf(cmd),
            CommandVerb::PSYNC => self.process_psync(config),
            CommandVerb::WAIT => self.process_wait(cmd),
        }
    }

    fn process_ping(&mut self) -> Option<PollResult> {
        self.stream.send_string(&String::from("+PONG\r\n"));
        println!("Sending PONG back");
        None
    }

    fn process_echo(&mut self, command: &[String]) -> Option<PollResult> {
        if let Some(message) = command.get(1) {
            let message = format!("${}\r\n{}\r\n", message.len(), message);
            self.stream.send_string(&message);
            None
        } else {
            None
        }
    }

    fn process_set(
        &mut self,
        command: &[String],
        global_state: &mut Cell<Store>,
    ) -> Option<PollResult> {
        let (Some(key), Some(value)) = (command.get(1), command.get(2)) else {
            return None;
        };

        let option = command.get(3);
        let option_value: Option<usize> = match command.get(4) {
            Some(option_value) => option_value.parse::<usize>().ok(),
            _ => None,
        };
        let ttl = match (option, option_value) {
            (Some(cmd), Some(cmd_value)) if cmd == "px" => Some(cmd_value),
            _ => None,
        };

        println!("Setting {}: {}", key, value);
        global_state.get_mut().set_string(key, value, ttl);
        if let ConnectionRole::Client = &self.connected_with {
            if self.replication.is_none() {
                self.stream.send_string(&String::from("+OK\r\n"));
            }
        };
        Some(PollResult::Write(format_array(&command.to_vec())))
    }

    fn process_get(
        &mut self,
        command: &[String],
        global_state: &mut Cell<Store>,
    ) -> Option<PollResult> {
        let key = command.get(1)?;
        self.stream
            .send_string(&format_string(global_state.get_mut().get_string(key)));
        None
    }

    fn process_type(
        &mut self,
        command: &[String],
        global_state: &mut Cell<Store>,
    ) -> Option<PollResult> {
        let key = command.get(1)?;
        let response = match global_state.get_mut().get_item_type(key) {
            None => "+none\r\n",
            Some(ItemType::String) => "+string\r\n",
            Some(ItemType::Stream) => "+stream\r\n",
        };

        self.stream.send_string(response);
        None
    }

    fn process_xadd(
        &mut self,
        command: &[String],
        global_state: &mut Cell<Store>,
    ) -> Option<PollResult> {
        let stream_key = command.get(1)?;
        let entry_id = parse_requested_stream_entry_id(command.get(2)?)?;

        let entries: IndexMap<String, String> = command[3..]
            .iter()
            .tuple_windows::<(_, _)>()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        match global_state
            .get_mut()
            .add_stream_entry(stream_key, &entry_id, &entries, None)
        {
            Ok(entry_id) => {
                println!("+{entry_id:?}\r\n");
                self.send_string(&format_string(Some(format!("{entry_id}"))));
                return Some(PollResult::WriteToStream(WriteToStream {
                    key: stream_key.to_owned(),
                    entry: StreamEntry {
                        id: entry_id,
                        values: entries,
                    },
                }));
            }
            Err(err) => self.send_string(&format!("-{err}\r\n")),
        };
        None
    }

    fn process_xrange(
        &mut self,
        command: &[String],
        global_state: &mut Cell<Store>,
    ) -> Option<PollResult> {
        let stream_key = command.get(1)?;
        let start_id = command.get(2).and_then(|s| parse_stream_entry_id(s));
        let end_id = command.get(3).and_then(|s| parse_stream_entry_id(s));

        let stream =
            global_state
                .get_mut()
                .get_stream_range(stream_key, start_id.as_ref(), end_id.as_ref());
        self.send_string(&format_stream(&stream));
        None
    }

    fn process_xread(
        &mut self,
        command: &[String],
        global_state: &mut Cell<Store>,
    ) -> Option<PollResult> {
        let option = command.get(1)?;
        if option != "streams" {
            return None;
        }
        let streams = parse_xread_streams_names(command);
        let mut message = format!("*{}\r\n", streams.len());
        for (stream, id) in streams {
            let stream_values = global_state
                .get_mut()
                .get_stream_range(&stream, id.as_ref(), None);
            message.push_str(&format!(
                "*2\r\n{}{}",
                format_string(Some(stream.clone())),
                format_stream(&stream_values)
            ));
        }
        self.send_string(&message);
        None
    }

    fn process_config(&mut self, command: &[String], config: &Config) -> Option<PollResult> {
        match command.get(1) {
            Some(action) if *action == "GET" => {
                let key = command.get(2)?;
                let value = config.get_arg(key)?.clone();
                let message = format!(
                    "*2\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                    key.len(),
                    key,
                    value.len(),
                    value
                );
                self.stream.send_string(&message);
                None
            }
            _ => panic!(),
        }
    }

    fn process_keys(
        &mut self,
        _command: &[String],
        global_state: &mut Cell<Store>,
    ) -> Option<PollResult> {
        let mut response = String::new();
        let keys = global_state.get_mut().get_keys();
        response.push_str(&format!("*{}\r\n", keys.len()));
        for key in keys {
            response.push_str(&format!("${}\r\n{}\r\n", key.len(), key));
        }
        self.stream.send_string(&response);
        None
    }

    fn process_info(&mut self, command: &[String], config: &Config) -> Option<PollResult> {
        match command.get(1) {
            Some(section) if *section == "replication" => {
                let mut response = String::new();
                let role = match config.replication.role {
                    ReplicationRole::Master => String::from("master"),
                    ReplicationRole::Replica(_) => String::from("slave"),
                };
                response.push_str(&format!("role:{role}\r\n"));
                response.push_str(&format!("master_replid:{}\r\n", config.replication.replid));
                response.push_str(&format!(
                    "master_repl_offset:{}\r\n",
                    config.replication.repl_offset
                ));
                self.stream.send_string(&format_string(Some(response)));
                None
            }
            _ => panic!(),
        }
    }

    fn process_replconf(&mut self, command: &[String]) -> Option<PollResult> {
        match command.get(1) {
            Some(option) if option == "ACK" => {
                if let Some(replication) = &mut self.replication {
                    replication.match_offsets();
                }
                Some(PollResult::AckSuccessful)
            }
            _ => {
                self.send_string(&String::from("+OK\r\n"));
                None
            }
        }
    }

    fn process_psync(&mut self, config: &Config) -> Option<PollResult> {
        self.stream.send_string(&format_string(Some(format!(
            "+FULLRESYNC {} {}",
            config.replication.replid, config.replication.repl_offset
        ))));

        let empty_db = fs::read("empty.rdb").ok()?;

        self.stream.send_string(&format!("${}\r\n", empty_db.len()));
        self.stream.send_bytes(&empty_db);
        Some(PollResult::PromoteToReplica)
    }

    fn process_wait(&mut self, command: &[String]) -> Option<PollResult> {
        let expected_number_of_replicas = command.get(1).and_then(|n| n.parse::<usize>().ok());
        let ttl = command.get(2).and_then(|n| n.parse::<usize>().ok());
        match (expected_number_of_replicas, ttl) {
            (Some(n_replicas), ttl) => {
                self.wait_for_replication_ack = true;
                Some(PollResult::WaitForAcks(ReplicationCheckRequest {
                    number_of_replicas: n_replicas,
                    timeout: ttl,
                }))
            }
            _ => {
                println!("Cannot process invalid WAIT command: {command:?}");
                None
            }
        }
    }

    pub fn is_replication_offset_aligned(&self) -> bool {
        println!("{:?}", self.replication);
        self.replication
            .as_ref()
            .map(|r| !r.need_to_ask_for_replication_ack())
            .unwrap_or(false)
    }
}

fn parse_requested_stream_entry_id(arg: &str) -> Option<RequestedStreamEntryId> {
    if arg == "*" {
        return Some(RequestedStreamEntryId::AutoGenerate);
    }

    let (first, second) = arg.split_at_checked(arg.find("-")?)?;
    let timestamp = first.parse::<usize>().ok()?;
    let second = second.strip_prefix("-")?;

    if second == "*" {
        return Some(RequestedStreamEntryId::AutoGenerateSequence(timestamp));
    }

    let sequence_number = second.parse::<usize>().ok()?;
    Some(RequestedStreamEntryId::Explicit(StreamEntryId {
        timestamp,
        sequence_number,
    }))
}

fn parse_stream_entry_id(arg: &str) -> Option<StreamEntryId> {
    if arg == "+" || arg == "-" {
        return None;
    }

    let (first, second) = arg.split_at_checked(arg.find("-")?)?;
    let timestamp = first.parse::<usize>().ok()?;

    let sequence_number = second.strip_prefix("-")?.parse::<usize>().ok()?;
    Some(StreamEntryId {
        timestamp,
        sequence_number,
    })
}

fn parse_xread_streams_names(cmd: &[String]) -> Vec<(String, Option<StreamEntryId>)> {
    let cmd = &cmd[2..];
    let midpoint = cmd.len() / 2;
    let names = cmd[..midpoint].iter();
    let ids = cmd[midpoint..].iter();

    zip(names, ids)
        .map(|(name, id)| (name.clone(), parse_stream_entry_id(id)))
        .collect()
}

#[cfg(test)]
mod tests {
    use crate::{
        master::connection::{parse_requested_stream_entry_id, parse_xread_streams_names},
        store::stream::{RequestedStreamEntryId, StreamEntryId},
    };

    #[test]
    fn requested_stream_entry_id_invalid() {
        let arg = String::from("toto");
        assert_eq!(parse_requested_stream_entry_id(&arg), None);
    }

    #[test]
    fn requested_stream_entry_id_auto_generate() {
        let arg = String::from("*");
        assert_eq!(
            parse_requested_stream_entry_id(&arg),
            Some(RequestedStreamEntryId::AutoGenerate)
        );
    }

    #[test]
    fn requested_stream_entry_id_auto_generate_sequence() {
        let arg = String::from("1526919030474-*");
        assert_eq!(
            parse_requested_stream_entry_id(&arg),
            Some(RequestedStreamEntryId::AutoGenerateSequence(1526919030474))
        );
    }

    #[test]
    fn requested_stream_entry_id_explicit() {
        let arg = String::from("1526919030474-12");
        assert_eq!(
            parse_requested_stream_entry_id(&arg),
            Some(RequestedStreamEntryId::Explicit(StreamEntryId {
                timestamp: 1526919030474,
                sequence_number: 12
            }))
        );
    }

    #[test]
    fn parse_xread_command() {
        let cmd: Vec<String> = String::from("XREAD streams stream_key other_stream_key 0-0 0-1")
            .split(" ")
            .map(|s| s.to_string())
            .collect();

        let res = parse_xread_streams_names(&cmd);
        let expected_res = vec![
            (
                String::from("stream_key"),
                Some(StreamEntryId {
                    timestamp: 0,
                    sequence_number: 0,
                }),
            ),
            (
                String::from("other_stream_key"),
                Some(StreamEntryId {
                    timestamp: 0,
                    sequence_number: 1,
                }),
            ),
        ];
        assert_eq!(res, expected_res)
    }
}
