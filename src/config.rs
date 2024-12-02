use std::{collections::HashMap, env};

type Args = HashMap<String, String>;

pub struct Config {
    pub port: i32,
    pub replication_role: ReplicationRole,
    pub dbfile: Option<DBFile>,
    args: Args,
}

impl Config {
    pub fn get_arg(&self, key: &str) -> Option<String> {
        self.args.get(key).cloned()
    }
}

pub enum ReplicationRole {
    Master,
    Replica(String),
}

#[derive(Clone)]
pub struct DBFile {
    pub dir: String,
    pub dbfilename: String,
}

pub fn parse_config() -> Config {
    let args = parse_args();

    let port = args
        .get("port")
        .map_or(6379, |value| value.parse::<i32>().unwrap_or(6379));

    let dbfile = dbfile_config(&args);

    let replication_role = args
        .get("replicaof")
        .map(|master| ReplicationRole::Replica(master.clone()))
        .unwrap_or(ReplicationRole::Master);

    Config {
        port,
        dbfile,
        replication_role,
        args,
    }
}

fn parse_args() -> Args {
    let mut args_iter = env::args();
    let mut args: Args = HashMap::new();

    // Drop first args, see `env::args()`
    let _ = args_iter.next();

    while let (Some(cmd), Some(param)) = (args_iter.next(), args_iter.next()) {
        let (prefix, cmd) = cmd.split_at(2);
        if prefix == "--" {
            args.insert(cmd.to_string(), param);
        }
    }

    args
}

fn dbfile_config(args: &Args) -> Option<DBFile> {
    if let (Some(dir), Some(dbfilename)) = (args.get("dir"), args.get("dbfilename")) {
        return Some(DBFile {
            dir: dir.clone(),
            dbfilename: dbfilename.clone(),
        });
    }
    None
}
