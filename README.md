# datagenerator

Generate random events and send them to MQTT

## Build

```
cargo build --release
```

## Usage

```
datagen 0.1
Mariano Guerra <mariano@marianoguerra.org>
Generate data and send it to an MQTT broker

USAGE:
    datagen [SUBCOMMAND]

FLAGS:
    -h, --help       Prints help information
    -V, --version    Prints version information

SUBCOMMANDS:
    help        Prints this message or the help of the given subcommand(s)
    mqtt-gen    Generate data and send it to an MQTT broker
```

For now only `mqtt-gen` subcommand available:

```
gen-mqtt-gen
Generate data and send it to an MQTT broker

USAGE:
    datagen mqtt-gen [OPTIONS]

FLAGS:
        --help       Prints help information
    -V, --version    Prints version information

OPTIONS:
    -g <GENERATOR_ID>        Generator type to use
    -c <PATH>                Path to config file to setup generator
    -h <HOST>                MQTT broker host
    -i <MS>                  Sleep [interval] between messages
    -p <PASSWORD>            MQTT broker authentication password
    -t <TOPIC>               MQTT broker topic
    -u <USERNAME>            MQTT broker authentication username
```

Examples:

```
datagen mqtt-gen -u mariano -p secret -t test -g id_event
datagen mqtt-gen -u mariano -p secret -t test -g id_event -c resources/empty-config.toml
datagen mqtt-gen -u mariano -p secret -t test -g id_event -c resources/id_event.toml

datagen mqtt-gen -u mariano -p secret -t test -g counter
datagen mqtt-gen -u mariano -p secret -t test -g counter -c resources/empty-config.toml
datagen mqtt-gen -u mariano -p secret -t test -g counter -c resources/counter.toml
```

## Config Examples

### id event generator

```
ids = ["Main Entrance", "Meeting Room",
       "Elevator Floor 1", "Elevator Floor 2", "Elevator Floor 3",
       "Stairs Floor 1", "Stairs Floor 2", "Stairs Floor 3"]
events = ["Open", "Close", "Enter", "Leave"]
format = "CSV" # or "JSON"
```

See resources/id\_event.toml

### counter generator

```
initial_count = 42
format = "JSON" # or "CSV"
```

See resources/counter.toml

## Tips

To test you need an MQTT broker, we recommend: https://vernemq.com/

Also a client to listen to published events, we recommend mosquitto-clients:

```
sudo apt install mosquitto-clients

mosquitto_sub -h localhost -p 1883 -t test -u mariano -P secret
```


## License

MIT

See LICENSE file for details
