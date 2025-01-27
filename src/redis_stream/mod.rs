use crate::store::stream::StreamEntry;

pub mod fmt;
pub mod parser;
pub mod stream;

#[derive(PartialEq)]
pub enum PollResult {
    Write(String),
    WriteToStream(WriteToStream),
    PromoteToReplica,
    WaitForAcks(ReplicationCheckRequest),
    AckSuccessful,
}

#[derive(PartialEq)]
pub struct WriteToStream {
    pub key: String,
    pub entry: StreamEntry,
}

#[derive(PartialEq, Clone, Copy)]
pub struct ReplicationCheckRequest {
    pub number_of_replicas: usize,
    pub timeout: Option<usize>,
}
