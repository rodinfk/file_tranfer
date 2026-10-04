use anyhow::{Context, bail};
use clap::Parser;
use file_transfer::{
    DEFAULT_IDLE_TIMEOUT_SECS, DEFAULT_MAX_FILE_SIZE, DEFAULT_TRANSFER_TIMEOUT_SECS,
    MAX_TOKEN_LENGTH, MIN_TOKEN_LENGTH, STATUS_AUTH_FAILED, STATUS_BUSY, STATUS_OK,
    STATUS_REJECTED, encode_header, validate_file_name,
};
use std::{env, path::PathBuf, time::Duration};
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    time::{Instant, timeout, timeout_at},
};
use tracing::info;

#[derive(Debug, Parser)]
#[command(about = "Send one file to a file-transfer server")]
struct Args {
    file: PathBuf,

    #[arg(long, default_value = "127.0.0.1:7878")]
    server: std::net::SocketAddr,

    #[arg(long, default_value_t = DEFAULT_MAX_FILE_SIZE, value_parser = clap::value_parser!(u64).range(1..))]
    max_file_size: u64,

    #[arg(long, default_value_t = DEFAULT_IDLE_TIMEOUT_SECS, value_parser = clap::value_parser!(u64).range(1..=86400))]
    idle_timeout_secs: u64,

    #[arg(long, default_value_t = DEFAULT_TRANSFER_TIMEOUT_SECS, value_parser = clap::value_parser!(u64).range(1..=86400))]
    transfer_timeout_secs: u64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_logging();
    let args = Args::parse();
    let token = read_token()?;
    send(args, token).await
}

async fn send(args: Args, token: Vec<u8>) -> anyhow::Result<()> {
    let mut file = File::open(&args.file)
        .await
        .with_context(|| format!("cannot open file: {}", args.file.display()))?;
    let metadata = file
        .metadata()
        .await
        .with_context(|| format!("cannot read file metadata: {}", args.file.display()))?;
    if !metadata.is_file() {
        bail!("the input path is not a regular file");
    }
    let size = metadata.len();
    if size > args.max_file_size {
        bail!(
            "file is {size} bytes, exceeding the client limit of {} bytes",
            args.max_file_size
        );
    }

    let name = args
        .file
        .file_name()
        .and_then(|name| name.to_str())
        .context("file name must be valid UTF-8")?;
    validate_file_name(name).context("invalid file name")?;
    let name = name.to_owned();

    let idle_timeout = Duration::from_secs(args.idle_timeout_secs);
    let transfer_timeout = Duration::from_secs(args.transfer_timeout_secs);
    let mut stream = timeout(idle_timeout, TcpStream::connect(args.server))
        .await
        .context("timed out connecting to server")?
        .with_context(|| format!("cannot connect to {}", args.server))?;
    stream.set_nodelay(true)?;

    let status = timeout(idle_timeout, async {
        stream.write_u16(token.len() as u16).await?;
        stream.write_all(&token).await?;
        let status = stream.read_u8().await?;
        anyhow::Ok(status)
    })
    .await
    .context("timed out during authentication")??;
    check_status(status, "authentication", "server rejected the token")?;

    let header = encode_header(&name, size)?;
    let status = timeout(idle_timeout, async {
        stream.write_all(&header).await?;
        let status = stream.read_u8().await?;
        anyhow::Ok(status)
    })
    .await
    .context("timed out waiting for server readiness")??;
    check_status(status, "server readiness", "server rejected the file")?;

    let deadline = Instant::now() + transfer_timeout;
    let mut buffer = vec![0u8; 64 * 1024];
    let mut remaining = size;
    while remaining > 0 {
        let read_len = remaining.min(buffer.len() as u64) as usize;
        let read = timeout_at(deadline, file.read(&mut buffer[..read_len]))
            .await
            .context("file transfer timed out while reading the source file")??;
        if read == 0 {
            bail!("source file ended before the declared size was sent");
        }
        timeout_at(deadline, stream.write_all(&buffer[..read]))
            .await
            .context("file transfer timed out while sending data")??;
        remaining -= read as u64;
    }

    let status = timeout_at(deadline, stream.read_u8())
        .await
        .context("timed out waiting for transfer confirmation")??;
    check_status(status, "transfer", "server could not store the file")?;
    info!(file = %args.file.display(), bytes = size, server = %args.server, "transfer completed");
    Ok(())
}

fn check_status(status: u8, phase: &str, rejected_message: &str) -> anyhow::Result<()> {
    match status {
        STATUS_OK => Ok(()),
        STATUS_AUTH_FAILED => bail!("{phase}: authentication failed"),
        STATUS_BUSY => bail!("{phase}: server is at its connection limit"),
        STATUS_REJECTED => bail!("{phase}: {rejected_message}"),
        status => bail!("{phase}: server returned unknown status {status}"),
    }
}

fn read_token() -> anyhow::Result<Vec<u8>> {
    let token = env::var("FILE_TRANSFER_TOKEN")
        .context("set FILE_TRANSFER_TOKEN in the environment on both client and server")?;
    let bytes = token.into_bytes();
    if !(MIN_TOKEN_LENGTH..=MAX_TOKEN_LENGTH).contains(&bytes.len()) {
        bail!(
            "FILE_TRANSFER_TOKEN must contain between {MIN_TOKEN_LENGTH} and {MAX_TOKEN_LENGTH} UTF-8 bytes"
        );
    }
    Ok(bytes)
}

fn init_logging() {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();
}
