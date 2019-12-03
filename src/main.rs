use clap::{App, Arg, SubCommand};
use paho_mqtt as mqtt;

use futures::Future;
use std::process;

const QOS: i32 = 1;

fn main() {
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
    let host = submatches.value_of("host").unwrap_or("tcp://localhost:1883");
    let topic = submatches.value_of("topic").unwrap_or("my-topic");
    let username = submatches.value_of("username").unwrap_or("myusername");
    let password = submatches.value_of("password").unwrap_or("mypassword");
    gen(host, topic, username, password);
}

fn gen(host: &str, topic: &str, username: &str, password: &str) {
    println!("mqtt-gen host: {host} topic: {topic} username: {username}", host=host, topic=topic, username=username);
    let conn_opts = mqtt::ConnectOptionsBuilder::new()
        .user_name(username)
        .password(password)
        .finalize();

    let cli = mqtt::AsyncClient::new(host).unwrap_or_else(|err| {
        println!("Error creating the client: {}", err);
        process::exit(1);
    });

    // Connect and wait for it to complete or fail
    if let Err(e) = cli.connect(conn_opts).wait() {
        println!("Unable to connect: {:?}", e);
        process::exit(1);
    }

    // Create a topic and publish to it
    let topic = mqtt::Topic::new(&cli, topic, QOS);
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
