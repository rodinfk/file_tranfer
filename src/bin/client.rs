use file_transfer::encode_header;
use std::{
    fs::File,
    io::{self, Read, Write},
    net::TcpStream,
    path::Path,
};

fn send(addr: &str, path: &Path) -> io::Result<()> {
    let mut file = File::open(path)?;

    let size = file.metadata()?.len();

    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "invalid file name"))?;

    let mut socket = TcpStream::connect(addr)?;

    socket.write_all(&encode_header(name, size)?)?;

    let sent = io::copy(&mut file, &mut socket)?;

    if sent != size {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "file changed while it was being sent",
        ));
    }

    socket.flush()?;

    let mut status = [0u8; 1];
    socket.read_exact(&mut status)?;

    if status[0] != 0 {
        return Err(io::Error::new(
            io::ErrorKind::Other,
            "server rejected the transfer",
        ));
    }

    Ok(())
}

fn main() -> io::Result<()> {
    let mut args = std::env::args().skip(1);
    let path = args.next().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "usage: client <file> [address]",
        )
    })?;
    let addr = args.next().unwrap_or_else(|| "127.0.0.1:7878".into());

    send(&addr, Path::new(&path))?;
    println!("sent {path} to {addr}");
    Ok(())
}
