use actor::{build_and_run_master, build_and_run_replica};
use config::{parse_config, Config, DBFile, ReplicationRole};
use store::Store;

pub mod actor;
pub mod config;
pub mod redis_stream;
pub mod store;

fn main() {
    println!("Logs from your program will appear here!");
    let config = parse_config();

    if let ReplicationRole::Replica(_) = &config.replication.role {
        build_and_run_master();
    } else {
        build_and_run_replica();
    }
}

fn build_store(config: &Config) -> Store {
    if let Some(DBFile { dir, dbfilename }) = &config.dbfile {
        if let Some(store) = Store::from_dbfile(dir, dbfilename) {
            return store;
        }
    }
    Store::new()
}
