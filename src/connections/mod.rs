pub mod fmt;
pub mod parser;
pub mod stream;

#[derive(PartialEq)]
pub enum PollResult {
    Write(String),
    PromoteToReplica,
    WaitForAcks(ReplicationCheckRequest),
    AckSuccessful,
}

#[derive(PartialEq, Clone, Copy)]
pub struct ReplicationCheckRequest {
    pub number_of_replicas: usize,
    pub timeout: Option<usize>,
}
