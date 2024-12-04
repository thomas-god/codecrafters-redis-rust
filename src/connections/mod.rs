pub mod client;

#[derive(PartialEq)]
pub enum PollResult {
    Write(String),
    PromoteToReplica,
}
