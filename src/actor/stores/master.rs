use std::{
    fs,
    iter::zip,
    sync::mpsc::{channel, Receiver, Sender},
    time::{Duration, Instant},
};

use indexmap::IndexMap;
use itertools::Itertools;

use crate::{
    actor::{ConnectionMessage, StoreMessage},
    config::{Config, ReplicationRole},
    connections::{
        fmt::{format_array, format_stream, format_string},
        parser::{BufferType, Command, CommandVerb},
    },
    store::{
        stream::{RequestedStreamEntryId, StreamEntryId},
        ItemType, Store,
    },
};

#[derive(Debug)]
struct Replication {
    replication_offset: usize,
    last_offset_checked: usize,
}

impl Replication {
    fn match_offsets(&mut self) {
        self.last_offset_checked = self.replication_offset;
    }
}

impl Default for Replication {
    fn default() -> Self {
        Replication {
            replication_offset: 0,
            last_offset_checked: 0,
        }
    }
}

struct WaitForReplicationAcks {
    initial_client_tx: Sender<ConnectionMessage>,
    expected_number_of_acks: usize,
    number_of_acks: usize,
    timeout: Option<Instant>,
}

pub struct MasterActor {
    store: Store,
    config: Config,
    tx: Sender<StoreMessage>,
    rx: Receiver<StoreMessage>,
    replication: Replication,
    replicas: Vec<Sender<ConnectionMessage>>,
    wait_for_replication_acks: Option<WaitForReplicationAcks>,
}

impl MasterActor {
    pub fn new(store: Store, config: Config) -> MasterActor {
        let (tx, rx) = channel();
        let replicas: Vec<Sender<ConnectionMessage>> = vec![];

        MasterActor {
            store,
            config,
            tx,
            rx,
            replication: Replication::default(),
            replicas,
            wait_for_replication_acks: None,
        }
    }

    pub fn poll(&mut self) {
        while let Ok(message) = self.rx.try_recv() {
            match message {
                StoreMessage::NewBuffer {
                    value: BufferType::Command(cmd),
                    tx_back,
                } => {
                    println!("{cmd:?}");
                    self.process_command(cmd, tx_back);
                }
                _ => todo!(),
            }
        }

        self.check_on_replication_waits();
    }

    pub fn get_tx(&self) -> Sender<StoreMessage> {
        self.tx.clone()
    }

    fn process_command(&mut self, cmd: Command, tx_back: Sender<ConnectionMessage>) {
        let Command { verb, cmd } = cmd;
        match verb {
            CommandVerb::PING => self.process_ping(tx_back),
            CommandVerb::ECHO => self.process_echo(&cmd, tx_back),
            CommandVerb::SET => self.process_set(&cmd, tx_back),
            CommandVerb::GET => self.process_get(&cmd, tx_back),
            CommandVerb::TYPE => self.process_type(&cmd, tx_back),
            CommandVerb::XADD => self.process_xadd(&cmd, tx_back),
            CommandVerb::XRANGE => self.process_xrange(&cmd, tx_back),
            CommandVerb::XREAD => self.process_xread(&cmd, tx_back),
            CommandVerb::CONFIG => self.process_config(&cmd, tx_back),
            CommandVerb::KEYS => self.process_keys(tx_back),
            CommandVerb::INFO => self.process_info(&cmd, tx_back),
            CommandVerb::REPLCONF => self.process_replconf(&cmd, tx_back),
            CommandVerb::PSYNC => self.process_psync(tx_back),
            CommandVerb::WAIT => self.process_wait(&cmd, tx_back),
        };
    }

    fn process_ping(&mut self, tx_back: Sender<ConnectionMessage>) {
        tx_back
            .send(ConnectionMessage::SendString(String::from("+PONG\r\n")))
            .unwrap();
    }

    fn process_echo(&mut self, command: &[String], tx_back: Sender<ConnectionMessage>) {
        if let Some(message) = command.get(1) {
            let message = format!("${}\r\n{}\r\n", message.len(), message);
            tx_back
                .send(ConnectionMessage::SendString(message))
                .unwrap();
        }
    }

    fn process_set(&mut self, command: &[String], tx_back: Sender<ConnectionMessage>) {
        let (Some(key), Some(value)) = (command.get(1), command.get(2)) else {
            return;
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
        self.store.set_string(key, value, ttl);
        tx_back
            .send(ConnectionMessage::SendString(String::from("+OK\r\n")))
            .unwrap();

        // Update replication offset and propagate to connected replicas
        self.replication.replication_offset +=
            command.iter().fold(0, |acc, s| acc + s.as_bytes().len());
        for tx_replica in &self.replicas {
            tx_replica
                .send(ConnectionMessage::SendString(format_array(
                    &command.to_vec(),
                )))
                .unwrap();
        }
    }

    fn process_get(&mut self, command: &[String], tx_back: Sender<ConnectionMessage>) {
        let Some(key) = command.get(1) else {
            return;
        };
        let value = self.store.get_string(key);
        let message = ConnectionMessage::SendString(format_string(value));
        tx_back.send(message).unwrap();
    }

    fn process_type(&mut self, command: &[String], tx_back: Sender<ConnectionMessage>) {
        let Some(key) = command.get(1) else {
            return;
        };
        let response = match self.store.get_item_type(key) {
            None => "+none\r\n",
            Some(ItemType::String) => "+string\r\n",
            Some(ItemType::Stream) => "+stream\r\n",
        };

        tx_back
            .send(ConnectionMessage::SendString(response.to_owned()))
            .unwrap();
    }

    fn process_xadd(&mut self, command: &[String], tx_back: Sender<ConnectionMessage>) {
        let Some(stream_key) = command.get(1) else {
            return;
        };
        let Some(entry_id) = command
            .get(2)
            .map(parse_requested_stream_entry_id)
            .flatten()
        else {
            return;
        };

        let entries: IndexMap<String, String> = command[3..]
            .iter()
            .tuple_windows::<(_, _)>()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        match self
            .store
            .add_stream_entry(stream_key, &entry_id, &entries, None)
        {
            Ok(entry_id) => {
                println!("+{entry_id:?}\r\n");
                tx_back
                    .send(ConnectionMessage::SendString(format_string(Some(format!(
                        "{entry_id}"
                    )))))
                    .unwrap();
                // return Some(PollResult::WriteToStream(WriteToStream {
                //     key: stream_key.to_owned(),
                //     entry: StreamEntry {
                //         id: entry_id,
                //         values: entries,
                //     },
                // }));
            }
            Err(err) => {
                tx_back
                    .send(ConnectionMessage::SendString(format!("-{err}\r\n")))
                    .unwrap();
            }
        };
    }

    fn process_xrange(&mut self, command: &[String], tx_back: Sender<ConnectionMessage>) {
        let Some(stream_key) = command.get(1) else {
            return;
        };
        let start_id = command.get(2).and_then(|s| parse_stream_entry_id(s));
        let end_id = command.get(3).and_then(|s| parse_stream_entry_id(s));

        let stream = self
            .store
            .get_stream_range(stream_key, start_id.as_ref(), end_id.as_ref());
        tx_back
            .send(ConnectionMessage::SendString(format_stream(&stream)))
            .unwrap();
    }

    fn process_xread(&mut self, command: &[String], tx_back: Sender<ConnectionMessage>) {
        let Some(option) = command.get(1) else {
            return;
        };
        if option != "streams" {
            return;
        }
        let streams = parse_xread_streams_names(command);
        let mut message = format!("*{}\r\n", streams.len());
        for (stream, id) in streams {
            let stream_values = self.store.get_stream_range(&stream, id.as_ref(), None);
            message.push_str(&format!(
                "*2\r\n{}{}",
                format_string(Some(stream.clone())),
                format_stream(&stream_values)
            ));
        }
        tx_back
            .send(ConnectionMessage::SendString(message))
            .unwrap();
    }

    fn process_config(&mut self, command: &[String], tx_back: Sender<ConnectionMessage>) {
        let (Some(action), Some(key)) = (command.get(1), command.get(2)) else {
            return;
        };
        if *action == "GET" {
            let Some(value) = self.config.get_arg(key) else {
                return;
            };
            let message = format!(
                "*2\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                key.len(),
                key,
                value.len(),
                value
            );
            tx_back
                .send(ConnectionMessage::SendString(message))
                .unwrap();
        }
    }

    fn process_keys(&mut self, tx_back: Sender<ConnectionMessage>) {
        let mut response = String::new();
        let keys = self.store.get_keys();
        response.push_str(&format!("*{}\r\n", keys.len()));
        for key in keys {
            response.push_str(&format!("${}\r\n{}\r\n", key.len(), key));
        }
        tx_back
            .send(ConnectionMessage::SendString(response))
            .unwrap();
    }

    fn process_info(&mut self, command: &[String], tx_back: Sender<ConnectionMessage>) {
        match command.get(1) {
            Some(section) if *section == "replication" => {
                let mut response = String::new();
                let role = match self.config.replication.role {
                    ReplicationRole::Master => String::from("master"),
                    ReplicationRole::Replica(_) => String::from("slave"),
                };
                response.push_str(&format!("role:{role}\r\n"));
                response.push_str(&format!(
                    "master_replid:{}\r\n",
                    self.config.replication.replid
                ));
                response.push_str(&format!(
                    "master_repl_offset:{}\r\n",
                    self.config.replication.repl_offset
                ));
                tx_back
                    .send(ConnectionMessage::SendString(format_string(Some(response))))
                    .unwrap();
            }
            _ => panic!(),
        }
    }

    fn process_replconf(&mut self, command: &[String], tx_back: Sender<ConnectionMessage>) {
        match command.get(1) {
            Some(option) if option == "ACK" => {
                if let Some(ref mut replication_task) = self.wait_for_replication_acks {
                    replication_task.number_of_acks += 1;
                }
                // self.replication.match_offsets();
            }
            _ => {
                tx_back
                    .send(ConnectionMessage::SendString(String::from("+OK\r\n")))
                    .unwrap();
            }
        }
    }

    fn process_psync(&mut self, tx_back: Sender<ConnectionMessage>) {
        tx_back
            .send(ConnectionMessage::SendString(format_string(Some(format!(
                "+FULLRESYNC {} {}",
                self.config.replication.replid, self.config.replication.repl_offset
            )))))
            .unwrap();

        let Ok(empty_db) = fs::read("empty.rdb") else {
            return;
        };

        tx_back
            .send(ConnectionMessage::SendString(format!(
                "${}\r\n",
                empty_db.len()
            )))
            .unwrap();
        tx_back
            .send(ConnectionMessage::SendBytes(empty_db))
            .unwrap();
        self.replicas.push(tx_back.clone());
    }

    fn process_wait(&mut self, command: &[String], tx_back: Sender<ConnectionMessage>) {
        let Some(expected_number_of_acks) = command.get(1).and_then(|n| n.parse::<usize>().ok())
        else {
            println!("Cannot process invalid WAIT command: {command:?}");
            return;
        };

        // Edge case: if the number of acks the client wants is 0, we can respond immediately with 0.
        if expected_number_of_acks == 0 {
            tx_back
                .send(ConnectionMessage::SendString(String::from(":0\r\n")))
                .unwrap();
            return;
        }

        // Edge case: if the last acked offset has not changed, we can respond immediately with the
        // number of replicas currently connected to the master instance.
        println!(
            "Replication offset: {} (last checked: {})",
            self.replication.replication_offset, self.replication.last_offset_checked
        );
        if self.replication.last_offset_checked == self.replication.replication_offset {
            tx_back
                .send(ConnectionMessage::SendString(format!(
                    ":{}\r\n",
                    self.replicas.len()
                )))
                .unwrap();
            return;
        }

        // Else, we send all replicas a REPLCONF GETACK * command.
        for replica in &self.replicas {
            replica
                .send(ConnectionMessage::SendString(format_array(&vec![
                    "REPLCONF".to_owned(),
                    "GETACK".to_owned(),
                    "*".to_owned(),
                ])))
                .unwrap();
        }

        let timeout = command
            .get(2)
            .and_then(|n| n.parse::<u64>().ok())
            .map(|ms| Instant::now() + Duration::from_millis(ms));
        self.wait_for_replication_acks = Some(WaitForReplicationAcks {
            expected_number_of_acks,
            initial_client_tx: tx_back,
            timeout,
            number_of_acks: 0,
        });
    }

    fn check_on_replication_waits(&mut self) {
        let Some(ref task) = self.wait_for_replication_acks else {
            return;
        };

        if let Some(timeout) = task.timeout {
            if timeout <= Instant::now() {
                task.initial_client_tx
                    .send(ConnectionMessage::SendString(format!(
                        ":{}\r\n",
                        task.number_of_acks
                    )))
                    .unwrap();
                self.replication.match_offsets();
                self.wait_for_replication_acks = None;
                return;
            }
        }

        if task.number_of_acks >= task.expected_number_of_acks {
            task.initial_client_tx
                .send(ConnectionMessage::SendString(format!(
                    ":{}\r\n",
                    task.number_of_acks
                )))
                .unwrap();
            self.replication.match_offsets();
            self.wait_for_replication_acks = None;
        }
    }
}

fn parse_requested_stream_entry_id(arg: &String) -> Option<RequestedStreamEntryId> {
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
        actor::stores::master::{parse_requested_stream_entry_id, parse_xread_streams_names},
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
