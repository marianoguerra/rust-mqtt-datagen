fn main() {
    match datagen::parse_args() { 
        Ok(gen_opts) => {datagen::gen(gen_opts)}
        Err(err) => eprintln!("Error: {:?}", err)
    }
}

