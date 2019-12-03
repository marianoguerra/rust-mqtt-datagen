use clap::{App, Arg, SubCommand};
use paho_mqtt as mqtt;

use futures::Future;
use std::process;

#[derive(Debug)]
pub struct GenOpts {
    host: String,
    topic: String,
    authenticate: bool,
    username: String,
    password: String,
    qos: i32,
}

impl GenOpts {
    pub fn new(host: &str, topic: &str, username: &str, password: &str) -> GenOpts {
        GenOpts {
            qos: 1,
            host: host.to_string(),
            topic: topic.to_string(),
            authenticate: true,
            username: username.to_string(),
            password: password.to_string(),
        }
    }

    pub fn to_conn_opts(&self) -> mqtt::connect_options::ConnectOptions {
        let mut opts = mqtt::ConnectOptionsBuilder::new();

        if self.authenticate {
            opts.user_name(self.username.to_string()).password(self.password.to_string());
        }

        return opts.finalize();
    }
}

#[derive(Debug)]
pub enum Error {
    Unk(String),
}

pub fn parse_args() -> Result<GenOpts, Error> {
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

    Ok(GenOpts::new(host, topic, username, password))
}

pub fn gen(opts: GenOpts) {
    println!("mqtt-gen {:?}", opts);
    let conn_opts = opts.to_conn_opts();

    let cli = mqtt::AsyncClient::new(opts.host).unwrap_or_else(|err| {
        println!("Error creating the client: {}", err);
        process::exit(1);
    });

    // Connect and wait for it to complete or fail
    if let Err(e) = cli.connect(conn_opts).wait() {
        println!("Unable to connect: {:?}", e);
        process::exit(1);
    }

    // Create a topic and publish to it
    let topic = mqtt::Topic::new(&cli, opts.topic, opts.qos);
    for _ in 0..5 {
        let tok = topic.publish("Hello there");

        if let Err(e) = tok.wait() {
            println!("Error sending message: {:?}", e);
            break;
        }
    }

    // Disconnect from the broker
    let tok = cli.disconnect(None);
    tok.wait().unwrap();
}
