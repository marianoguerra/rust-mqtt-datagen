fn main() {
    match datagen::parse_args() {
        Ok(gen_opts) => match datagen::gen(gen_opts) {
            Ok(_) => {}
            Err(err) => eprintln!("Error: {:?}", err),
        },
        Err(err) => eprintln!("Error: {:?}", err),
    }
}
