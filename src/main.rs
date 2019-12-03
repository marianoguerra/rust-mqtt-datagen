use paho_mqtt as mqtt;

use futures::Future;
use std::process;

const QOS: i32 = 1;

fn main() {
    let host = "tcp://localhost:1883";
    let username = "mariano";
    let password = "secret";
    let conn_opts = mqtt::ConnectOptionsBuilder::new()
        .user_name(username)
        .password(password)
        .finalize();
    let topic = "test";
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
    println!("Publishing messages on the 'test' topic");
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
