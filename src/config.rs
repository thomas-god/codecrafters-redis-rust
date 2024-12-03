use std::{collections::HashMap, env};

type Args = HashMap<String, String>;

pub struct Config {
    pub port: i32,
    pub replication: Replication,
    pub dbfile: Option<DBFile>,
    args: Args,
}

impl Config {
    pub fn get_arg(&self, key: &str) -> Option<String> {
        self.args.get(key).cloned()
    }
}

#[derive(Debug)]
pub enum ReplicationRole {
    Master,
    Replica((String, String)),
}

#[derive(Debug)]
pub struct Replication {
    pub role: ReplicationRole,
    pub replid: String,
    pub repl_offset: usize,
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

    let replication_role = match args.get("replicaof") {
        Some(url) => {
            if let (Some(host), Some(port)) = (url.split(" ").next(), url.split(" ").nth(1)) {
                ReplicationRole::Replica((host.to_string(), port.to_string()))
            } else {
                ReplicationRole::Master
            }
        }
        None => ReplicationRole::Master,
    };
    println!("Replication role: {replication_role:?}");

    let replication = Replication {
        role: replication_role,
        repl_offset: 0,
        replid: String::from("8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"),
    };

    Config {
        port,
        dbfile,
        replication,
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
