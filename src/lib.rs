#[macro_use]
extern crate clap;
use clap::{App, Arg, SubCommand};
use paho_mqtt as mqtt;
use rand::seq::SliceRandom;
use serde;
use serde_derive::{Deserialize, Serialize};

use futures::Future;
use std::error::Error;
use std::fs::File;
use std::io::{self, Read};
use std::thread;
use std::time::Duration;

use rdkafka;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};

pub trait Generator {
    fn gen(&mut self) -> Result<String, Box<dyn Error>>;
}

#[derive(Debug)]
pub struct IdEventGen {
    ids: Vec<String>,
    events: Vec<String>,
    format: Format,
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
            format: Format::CSV,
        }
    }

    fn from_config(config: IdEventConfig) -> Self {
        Self {
            ids: config.ids,
            events: config.events,
            format: config.format.unwrap_or(Format::CSV),
        }
    }

    pub fn from_file(path: Option<&str>) -> io::Result<Self> {
        match path {
            Some(path_s) => {
                let content = read_file(path_s)?;
                let config: IdEventConfig = toml::from_str(&content)?;
                Ok(Self::from_config(config))
            }
            None => Ok(Self::new()),
        }
    }
}

fn read_file(path: &str) -> io::Result<String> {
    let mut file = File::open(path)?;
    let mut content = String::new();
    file.read_to_string(&mut content)?;
    return Ok(content);
}

#[derive(Debug, Deserialize)]
enum Format {
    CSV,
    JSON,
}

impl Format {
    fn serialize<T: serde::Serialize>(&self, v: T) -> Result<String, Box<dyn Error>> {
        match self {
            Format::CSV => serialize_to_csv(v),
            Format::JSON => serialize_to_json(v),
        }
    }
}

#[derive(Deserialize)]
struct IdEventConfig {
    ids: Vec<String>,
    events: Vec<String>,
    format: Option<Format>,
}

#[derive(Debug, Serialize)]
struct IdEventEntry {
    time: String,
    id: String,
    event: String,
}

fn serialize_to_csv<T: serde::Serialize>(v: T) -> Result<String, Box<dyn Error>> {
    let mut wtr = csv::Writer::from_writer(vec![]);
    wtr.serialize(v)?;
    let inner = wtr.into_inner()?;
    Ok(String::from_utf8(inner)?)
}

fn serialize_to_json<T: serde::Serialize>(v: T) -> Result<String, Box<dyn Error>> {
    Ok(serde_json::to_string(&v)?)
}

impl Generator for IdEventGen {
    fn gen(&mut self) -> Result<String, Box<dyn Error>> {
        let id = match self.ids.choose(&mut rand::thread_rng()) {
            Some(v) => v.to_string(),
            None => String::from("?"),
        };
        let event = match self.events.choose(&mut rand::thread_rng()) {
            Some(v) => v.to_string(),
            None => String::from("?"),
        };
        self.format.serialize(IdEventEntry {
            time: get_current_time_iso8601(),
            id,
            event,
        })
    }
}

#[derive(Debug)]
pub struct CounterGen {
    count: u64,
    format: Format,
}

#[derive(Deserialize)]
struct CounterConfig {
    initial_count: Option<u64>,
    format: Option<Format>,
}

impl CounterGen {
    pub fn new() -> Self {
        CounterGen {
            count: 0,
            format: Format::CSV,
        }
    }

    fn from_config(config: CounterConfig) -> Self {
        Self {
            count: config.initial_count.unwrap_or(0),
            format: config.format.unwrap_or(Format::CSV),
        }
    }

    pub fn from_file(path: Option<&str>) -> std::io::Result<Self> {
        match path {
            Some(path_s) => {
                let content = read_file(path_s)?;
                let config: CounterConfig = toml::from_str(&content)?;
                Ok(Self::from_config(config))
            }
            None => Ok(Self::new()),
        }
    }
}

#[derive(Debug, Serialize)]
struct CounterEntry {
    time: String,
    count: u64,
}

fn get_current_time_iso8601() -> String {
    let time = chrono::offset::Utc::now();
    time.format("%+").to_string()
}

impl Generator for CounterGen {
    fn gen(&mut self) -> Result<String, Box<dyn Error>> {
        let r = self.format.serialize(CounterEntry {
            time: get_current_time_iso8601(),
            count: self.count,
        });

        self.count += 1;

        return r;
    }
}

pub enum GenOpts {
    Mqtt {
        host: String,
        topic: String,
        authenticate: bool,
        username: String,
        password: String,
        qos: i32,
        generator: Box<dyn Generator>,
        sleep_interval: Duration,
    },
    Kafka {
        bootstrap_servers: String,
        topic: String,
        generator: Box<dyn Generator>,
        sleep_interval: Duration,
    },
}

impl GenOpts {
    pub fn new_mqtt(
        host: &str,
        topic: &str,
        username: &str,
        password: &str,
        sleep_interval: Duration,
        generator: Box<dyn Generator>,
    ) -> GenOpts {
        GenOpts::Mqtt {
            qos: 1,
            host: host.to_string(),
            topic: topic.to_string(),
            authenticate: true,
            username: username.to_string(),
            password: password.to_string(),
            sleep_interval,
            generator,
        }
    }

    pub fn new_kafka(
        bootstrap_servers: &str,
        topic: &str,
        sleep_interval: Duration,
        generator: Box<dyn Generator>,
    ) -> GenOpts {
        GenOpts::Kafka {
            bootstrap_servers: bootstrap_servers.to_string(),
            topic: topic.to_string(),
            sleep_interval,
            generator,
        }
    }

    pub fn to_string(&self) -> String {
        match self {
            GenOpts::Mqtt {
                host,
                topic,
                username,
                ..
            } => format!("GenOpts: {:} {:} {:}", host, topic, username),
            GenOpts::Kafka {
                bootstrap_servers,
                topic,
                ..
            } => format!("GenOpts: {:} {:}", bootstrap_servers, topic),
        }
    }

    fn sleep_interval(&self) -> Duration {
        match self {
            GenOpts::Mqtt { sleep_interval, .. } => sleep_interval.clone(),
            GenOpts::Kafka { sleep_interval, .. } => sleep_interval.clone(),
        }
    }
}

trait Sink {
    fn send(&self, data: String) -> Result<(), SinkErr>;
    fn close(&self) -> Result<(), SinkErr>;
    fn gen(&mut self) -> Result<String, Box<dyn Error>>;
}

fn new_sink(opts: GenOpts) -> Result<Box<dyn Sink>, SinkErr> {
    match opts {
        GenOpts::Mqtt {
            host,
            authenticate,
            username,
            password,
            topic,
            qos,
            generator,
            ..
        } => Ok(Box::new(MqttSink::new(
            &host,
            authenticate,
            &username,
            &password,
            &topic,
            qos,
            generator,
        )?)),
        GenOpts::Kafka {
            bootstrap_servers,
            topic,
            generator,
            ..
        } => Ok(Box::new(KafkaSink::new(
            &bootstrap_servers,
            &topic,
            generator,
        )?)),
    }
}

#[derive(Debug)]
pub enum SinkErr {
    Unk(String),
    NoGenerator(String),
    ConfigError(String, io::Error),
    MqttError(mqtt::errors::MqttError),
    KafkaError(rdkafka::error::KafkaError),
}

struct MqttSink {
    client: mqtt::AsyncClient,
    topic: String,
    qos: i32,
    generator: Box<dyn Generator>,
}

impl MqttSink {
    fn new(
        host: &str,
        authenticate: bool,
        username: &str,
        password: &str,
        topic: &str,
        qos: i32,
        generator: Box<dyn Generator>,
    ) -> Result<MqttSink, SinkErr> {
        let mut opts = mqtt::ConnectOptionsBuilder::new();

        if authenticate {
            opts.user_name(username.to_string())
                .password(password.to_string());
        }

        let conn_opts = opts.finalize();
        let client = mqtt::AsyncClient::new(host.clone())?;

        // Connect and wait for it to complete or fail
        if let Err(e) = client.connect(conn_opts).wait() {
            println!("Unable to connect: {:?}", e);
            return Err(SinkErr::MqttError(e));
        }

        return Ok(MqttSink {
            client,
            qos: qos,
            topic: topic.to_string(),
            generator,
        });
    }
}

impl Sink for MqttSink {
    fn send(&self, data: String) -> Result<(), SinkErr> {
        // see if we can keep it in the struct, I had lifetime problems trying
        // to do that
        let topic = mqtt::Topic::new(&self.client, self.topic.clone(), self.qos);

        topic.publish(data).wait()?;
        return Ok(());
    }

    fn close(&self) -> Result<(), SinkErr> {
        self.client.disconnect(None).wait()?;
        return Ok(());
    }

    fn gen(&mut self) -> Result<String, Box<dyn Error>> {
        self.generator.gen()
    }
}

struct KafkaSink {
    producer: FutureProducer,
    topic: String,
    generator: Box<dyn Generator>,
}

impl KafkaSink {
    fn new(
        bootstrap_servers: &str,
        topic: &str,
        generator: Box<dyn Generator>,
    ) -> Result<KafkaSink, SinkErr> {
        // TODO
        let msg_timeout_ms = "5000";
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", bootstrap_servers)
            .set("produce.offset.report", "true")
            .set("message.timeout.ms", msg_timeout_ms)
            .create()?;

        return Ok(KafkaSink {
            topic: topic.to_string(),
            producer,
            generator,
        });
    }
}

impl Sink for KafkaSink {
    fn send(&self, data: String) -> Result<(), SinkErr> {
        let record: FutureRecord<String, String> = FutureRecord::to(&self.topic).payload(&data);

        match self.producer.send_result(record) {
            Ok(_) => Ok(()),
            Err((err, _)) => Err(SinkErr::KafkaError(err)),
        }
    }

    fn close(&self) -> Result<(), SinkErr> {
        return Ok(());
    }

    fn gen(&mut self) -> Result<String, Box<dyn Error>> {
        self.generator.gen()
    }
}

#[derive(Debug)]
pub enum GenError {
    Unk(String),
    NoGenerator(String),
    NoSink,
    InvalidSink(String),
    ConfigError(String, io::Error),
}

impl From<mqtt::errors::MqttError> for SinkErr {
    fn from(error: mqtt::errors::MqttError) -> SinkErr {
        SinkErr::MqttError(error)
    }
}

impl From<rdkafka::error::KafkaError> for SinkErr {
    fn from(error: rdkafka::error::KafkaError) -> SinkErr {
        SinkErr::KafkaError(error)
    }
}

pub fn parse_args() -> Result<GenOpts, GenError> {
    let matches = App::new("datagen")
        .version("0.1")
        .author("Mariano Guerra <mariano@marianoguerra.org>")
        .about("Generate data and send it to an MQTT broker")
        .arg(
            Arg::with_name("interval")
                .short("i")
                .value_name("MS")
                .takes_value(true)
                .help("Sleep [interval] between messages"),
        )
        .arg(
            Arg::with_name("generator")
                .short("g")
                .value_name("GENERATOR_ID")
                .takes_value(true)
                .help("Generator type to use"),
        )
        .arg(
            Arg::with_name("generator-config")
                .short("c")
                .value_name("PATH")
                .takes_value(true)
                .help("Path to config file to setup generator"),
        )
        .subcommand(
            SubCommand::with_name("kafka-gen")
                .about("Generate data and send it to a Kafka broker")
                .arg(
                    Arg::with_name("bootstrap-servers")
                        .short("b")
                        .value_name("SERVERS")
                        .takes_value(true)
                        .help("Bootstrap server list"),
                )
                .arg(
                    Arg::with_name("topic")
                        .short("t")
                        .value_name("TOPIC")
                        .takes_value(true)
                        .help("MQTT broker topic"),
                ),
        )
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
                ),
        )
        .get_matches();

    // ------------

    // TODO: error handling
    let sleep_interval_ms = value_t!(matches, "interval", u64).unwrap_or(500);
    let sleep_interval = Duration::from_millis(sleep_interval_ms);
    let gen_type = matches.value_of("generator").unwrap_or("counter");
    let gen_config_path = matches.value_of("generator-config");

    match matches.subcommand_name() {
        Some("mqtt-gen") => {
            let submatches = matches.subcommand_matches("mqtt-gen").unwrap();
            let host = submatches
                .value_of("host")
                .unwrap_or("tcp://localhost:1883");
            let topic = submatches.value_of("topic").unwrap_or("my-topic");
            let username = submatches.value_of("username").unwrap_or("myusername");
            let password = submatches.value_of("password").unwrap_or("mypassword");

            match generator_from_id(gen_type, gen_config_path) {
                Ok(generator) => Ok(GenOpts::new_mqtt(
                    host,
                    topic,
                    username,
                    password,
                    sleep_interval,
                    generator,
                )),
                Err(err) => Err(err),
            }
        }
        Some("kafka-gen") => {
            let submatches = matches.subcommand_matches("kafka-gen").unwrap();
            let bootstrap_servers = submatches
                .value_of("bootstrap-servers")
                .unwrap_or("127.0.0.1:9092");
            let topic = submatches.value_of("topic").unwrap_or("my-topic");
            match generator_from_id(gen_type, gen_config_path) {
                Ok(generator) => Ok(GenOpts::new_kafka(
                    bootstrap_servers,
                    topic,
                    sleep_interval,
                    generator,
                )),
                Err(err) => Err(err),
            }
        }
        Some(other) => {
            eprintln!("Invalid sink: {}", other);
            Err(GenError::InvalidSink(other.to_string()))
        }
        None => {
            eprintln!("No sink selected");
            Err(GenError::NoSink)
        }
    }
}

fn generator_from_id(id: &str, path: Option<&str>) -> Result<Box<dyn Generator>, GenError> {
    match id {
        "counter" => match CounterGen::from_file(path) {
            Ok(generator) => Ok(Box::new(generator)),
            Err(err) => Err(GenError::ConfigError(path.unwrap_or("?").to_string(), err)),
        },
        "id_event" => match IdEventGen::from_file(path) {
            Ok(generator) => Ok(Box::new(generator)),
            Err(err) => Err(GenError::ConfigError(path.unwrap_or("?").to_string(), err)),
        },
        _ => Err(GenError::NoGenerator(id.to_string())),
    }
}

pub fn gen(opts: GenOpts) -> Result<(), SinkErr> {
    println!("gen {:}", opts.to_string());
    println!("Ctrl-c to quit");

    let sleep_interval = opts.sleep_interval();
    let mut sink = new_sink(opts)?;

    loop {
        match sink.gen() {
            Ok(data) => {
                if let Err(e) = sink.send(data) {
                    println!("Error sending message: {:?}", e);
                    break;
                }
            }
            Err(err) => {
                eprintln!("Error generating data: {:?}", err);
            }
        }

        thread::sleep(sleep_interval);
    }

    sink.close()?;
    return Ok(());
}
