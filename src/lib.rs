#[macro_use]
extern crate clap;
use clap::{App, Arg, SubCommand};
use paho_mqtt as mqtt;
use rand::seq::SliceRandom;
use serde_derive::Serialize;

use futures::Future;
use std::error::Error;
use std::thread;
use std::time::Duration;

pub trait Generator {
    fn gen(&self) -> Result<String, Box<dyn Error>>;
}

#[derive(Debug)]
pub struct IdEventGen {
    ids: Vec<String>,
    events: Vec<String>,
}

impl IdEventGen {
    pub fn new() -> IdEventGen {
        IdEventGen {
            ids: vec![
                "door-1".to_string(),
                "window-2".to_string(),
                "access-1".to_string(),
            ],
            events: vec!["open".to_string(), "close".to_string(), "cross".to_string()],
        }
    }
}

#[derive(Debug, Serialize)]
struct IdEventEntry {
    id: String,
    event: String,
}

impl Generator for IdEventGen {
    fn gen(&self) -> Result<String, Box<dyn Error>> {
        let id = match self.ids.choose(&mut rand::thread_rng()) {
            Some(v) => v.to_string(),
            None => String::from("?"),
        };
        let event = match self.events.choose(&mut rand::thread_rng()) {
            Some(v) => v.to_string(),
            None => String::from("?"),
        };
        let mut wtr = csv::Writer::from_writer(vec![]);
        wtr.serialize(IdEventEntry { id, event })?;
        let inner = wtr.into_inner()?;
        let data = String::from_utf8(inner)?;
        return Ok(data);
    }
}

pub struct GenOpts {
    host: String,
    topic: String,
    authenticate: bool,
    username: String,
    password: String,
    qos: i32,
    generator: Box<dyn Generator>,
    sleep_interval: Duration,
}

impl GenOpts {
    pub fn new(
        host: &str,
        topic: &str,
        username: &str,
        password: &str,
        sleep_interval: Duration,
    ) -> GenOpts {
        GenOpts {
            qos: 1,
            host: host.to_string(),
            topic: topic.to_string(),
            authenticate: true,
            username: username.to_string(),
            password: password.to_string(),
            generator: Box::new(IdEventGen::new()),
            sleep_interval: sleep_interval,
        }
    }

    pub fn to_string(&self) -> String {
        format!("GenOpts: {:} {:} {:}", self.host, self.topic, self.username)
    }

    pub fn to_conn_opts(&self) -> mqtt::connect_options::ConnectOptions {
        let mut opts = mqtt::ConnectOptionsBuilder::new();

        if self.authenticate {
            opts.user_name(self.username.to_string())
                .password(self.password.to_string());
        }

        return opts.finalize();
    }

    pub fn with_generator(&mut self, generator: Box<dyn Generator>) {
        self.generator = generator;
    }
}

#[derive(Debug)]
pub enum GenError {
    Unk(String),
    ConnInitErr(mqtt::errors::MqttError),
    ConnErr(mqtt::errors::MqttError),
}

impl From<mqtt::errors::MqttError> for GenError {
    fn from(error: mqtt::errors::MqttError) -> GenError {
        GenError::ConnErr(error)
    }
}

pub fn parse_args() -> Result<GenOpts, GenError> {
    let matches = App::new("datagen")
        .version("0.1")
        .author("Mariano Guerra <mariano@marianoguerra.org>")
        .about("Generate data and send it to an MQTT broker")
        .subcommand(
            SubCommand::with_name("mqtt-gen")
                .about("Generate data and send it to an MQTT broker")
                .arg(
                    Arg::with_name("host")
                        .short("h")
                        .value_name("HOST")
                        .takes_value(true)
                        .help("MQTT broker host"),
                )
                .arg(
                    Arg::with_name("topic")
                        .short("t")
                        .value_name("TOPIC")
                        .takes_value(true)
                        .help("MQTT broker topic"),
                )
                .arg(
                    Arg::with_name("username")
                        .short("u")
                        .value_name("USERNAME")
                        .takes_value(true)
                        .help("MQTT broker authentication username"),
                )
                .arg(
                    Arg::with_name("password")
                        .short("p")
                        .value_name("PASSWORD")
                        .takes_value(true)
                        .help("MQTT broker authentication password"),
                )
                .arg(
                    Arg::with_name("interval")
                        .short("i")
                        .value_name("MS")
                        .takes_value(true)
                        .help("Sleep [interval] between messages"),
                ),
        )
        .get_matches();

    // ------------

    // TODO: error handling
    let submatches = matches.subcommand_matches("mqtt-gen").unwrap();
    let host = submatches
        .value_of("host")
        .unwrap_or("tcp://localhost:1883");
    let topic = submatches.value_of("topic").unwrap_or("my-topic");
    let username = submatches.value_of("username").unwrap_or("myusername");
    let password = submatches.value_of("password").unwrap_or("mypassword");
    let sleep_interval_ms = value_t!(submatches, "interval", u64).unwrap_or(500);
    let sleep_interval = Duration::from_millis(sleep_interval_ms);

    Ok(GenOpts::new(
        host,
        topic,
        username,
        password,
        sleep_interval,
    ))
}

pub fn gen(opts: GenOpts) -> Result<(), GenError> {
    println!("mqtt-gen {:}", opts.to_string());
    println!("Ctrl-c to quit");

    let conn_opts = opts.to_conn_opts();

    let cli = mqtt::AsyncClient::new(opts.host)?;

    // Connect and wait for it to complete or fail
    if let Err(e) = cli.connect(conn_opts).wait() {
        println!("Unable to connect: {:?}", e);
        return Err(GenError::ConnErr(e));
    }

    // Create a topic and publish to it
    let topic = mqtt::Topic::new(&cli, opts.topic, opts.qos);
    loop {
        match opts.generator.gen() {
            Ok(data) => {
                let tok = topic.publish(data);
                if let Err(e) = tok.wait() {
                    println!("Error sending message: {:?}", e);
                    break;
                }
            }
            Err(err) => {
                eprintln!("Error generating data: {:?}", err);
            }
        }

        thread::sleep(opts.sleep_interval);
    }

    // Disconnect from the broker
    let tok = cli.disconnect(None);
    tok.wait().unwrap();
    return Ok(());
}
