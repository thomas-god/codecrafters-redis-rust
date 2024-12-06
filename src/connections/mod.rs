pub mod client;
pub mod parser;

#[derive(PartialEq)]
pub enum PollResult {
    Write(String),
    PromoteToReplica,
}
