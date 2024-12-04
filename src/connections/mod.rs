pub mod client;
pub mod master;

pub enum PollResult {
    Write(String),
    PromoteToReplica,
}
