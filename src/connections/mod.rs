pub mod client;
pub mod parser;

#[derive(PartialEq)]
pub enum PollResult {
    Write(String),
    PromoteToReplica,
    WaitForAcks((NumbeOfReplicas, Timeout))
}

type NumbeOfReplicas = usize;
type Timeout = usize;
