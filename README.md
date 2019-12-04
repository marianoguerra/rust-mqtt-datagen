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

## License

MIT

See LICENSE file for details