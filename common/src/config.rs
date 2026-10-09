use serde::de::DeserializeOwned;
use std::{error::Error, fs};

pub fn load_config<T: DeserializeOwned>(
    path: &str,
) -> Result<T, Box<dyn Error>> {
    let content = fs::read_to_string(path)?;
    let config = serde_json::from_str(&content)?;
    Ok(config)
}