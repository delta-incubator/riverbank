/*
 * The config module is responsible for deserializing the yaml configuration
 */
use serde::Deserialize;
use std::error::Error;
use std::fs::File;
use std::io::BufReader;
use std::path::Path;

#[derive(Clone, Debug, Deserialize)]
pub struct Config {}

impl Config {
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Config, Box<dyn Error>> {
        let file = File::open(path)?;
        let c = serde_yaml::from_reader(BufReader::new(file))?;

        Ok(c)
    }
}
