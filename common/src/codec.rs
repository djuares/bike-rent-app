use serde::{de::DeserializeOwned, Serialize};
use std::io::{self, BufRead, BufReader, Write};
use std::net::TcpStream;

pub fn encode<T: Serialize>(
    stream: &mut TcpStream,
    msg: &T,
) -> io::Result<()> {
    let line = serde_json::to_string(msg)
        .map_err(io::Error::other)?;

    stream.write_all(line.as_bytes())?;
    stream.write_all(b"\n")?;
    stream.flush()?;

    Ok(())
}

pub fn decode<T: DeserializeOwned>(
    stream: &mut TcpStream,
) -> io::Result<T> {
    let mut reader = BufReader::new(stream);

    let mut line = String::new();

    reader.read_line(&mut line)?;

    let msg =
        serde_json::from_str::<T>(line.trim_end())
            .map_err(io::Error::other)?;

    Ok(msg)
}