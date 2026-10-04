use anyhow::{Context, bail};
use clap::Parser;
use file_transfer::{
    DEFAULT_IDLE_TIMEOUT_SECS, DEFAULT_MAX_CONNECTIONS, DEFAULT_MAX_FILE_SIZE,
    DEFAULT_TRANSFER_TIMEOUT_SECS, MAX_NAME_LENGTH, MAX_TOKEN_LENGTH, MIN_TOKEN_LENGTH,
    STATUS_AUTH_FAILED, STATUS_BUSY, STATUS_OK, STATUS_REJECTED, validate_file_name,
};
use std::{
    env, io,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use subtle::ConstantTimeEq;
use tokio::{
    fs::{self, File, OpenOptions},
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{OwnedSemaphorePermit, Semaphore},
    task::JoinSet,
    time::{Instant, timeout, timeout_at},
};
use tracing::{error, info, warn};

static NEXT_TEMP_ID: AtomicU64 = AtomicU64::new(0);
const CHUNK_SIZE: usize = 64 * 1024;

#[derive(Debug, Parser)]
#[command(about = "Receive files over TCP from authenticated clients")]
struct Args {
    #[arg(long, default_value = "127.0.0.1:7878")]
    listen: SocketAddr,

    #[arg(long, default_value = "received")]
    output_dir: PathBuf,

    #[arg(long, default_value_t = DEFAULT_MAX_FILE_SIZE, value_parser = clap::value_parser!(u64).range(1..))]
    max_file_size: u64,

    #[arg(long, default_value_t = DEFAULT_MAX_CONNECTIONS as u32, value_parser = clap::value_parser!(u32).range(1..=4096))]
    max_connections: u32,

    #[arg(long, default_value_t = DEFAULT_IDLE_TIMEOUT_SECS, value_parser = clap::value_parser!(u64).range(1..=86400))]
    idle_timeout_secs: u64,

    #[arg(long, default_value_t = DEFAULT_TRANSFER_TIMEOUT_SECS, value_parser = clap::value_parser!(u64).range(1..=86400))]
    transfer_timeout_secs: u64,
}

#[derive(Clone)]
struct Config {
    token: Arc<[u8]>,
    output_dir: Arc<PathBuf>,
    max_file_size: u64,
    idle_timeout: Duration,
    transfer_timeout: Duration,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_logging();
    let args = Args::parse();
    let token = read_token()?;
    fs::create_dir_all(&args.output_dir)
        .await
        .with_context(|| {
            format!(
                "cannot create output directory {}",
                args.output_dir.display()
            )
        })?;
    let listener = TcpListener::bind(args.listen)
        .await
        .with_context(|| format!("cannot bind to {}", args.listen))?;
    let local_addr = listener.local_addr()?;
    info!(
        %local_addr,
        output_dir = %args.output_dir.display(),
        max_file_size = args.max_file_size,
        max_connections = args.max_connections,
        "server listening"
    );

    let config = Config {
        token: token.into(),
        output_dir: Arc::new(args.output_dir),
        max_file_size: args.max_file_size,
        idle_timeout: Duration::from_secs(args.idle_timeout_secs),
        transfer_timeout: Duration::from_secs(args.transfer_timeout_secs),
    };
    let connection_limit = Arc::new(Semaphore::new(
        usize::try_from(args.max_connections).context("connection limit does not fit usize")?,
    ));
    let mut tasks = JoinSet::new();
    let shutdown_signal = tokio::signal::ctrl_c();
    tokio::pin!(shutdown_signal);

    loop {
        tokio::select! {
            signal = &mut shutdown_signal => {
                signal.context("failed to listen for Ctrl+C")?;
                info!("shutdown requested; no longer accepting connections");
                break;
            }
            accepted = listener.accept() => {
                let (stream, peer) = accepted.context("failed to accept TCP connection")?;
                match Arc::clone(&connection_limit).try_acquire_owned() {
                    Ok(permit) => {
                        let config = config.clone();
                        tasks.spawn(async move {
                            if let Err(error) = handle_connection(stream, peer, config, permit).await {
                                warn!(%peer, %error, "connection ended with an error");
                            }
                        });
                    }
                    Err(_) => {
                        warn!(%peer, "connection limit reached");
                        reject_busy(stream);
                    }
                }
            }
            Some(result) = tasks.join_next(), if !tasks.is_empty() => {
                if let Err(error) = result {
                    error!(%error, "connection task failed");
                }
            }
        }
    }

    while let Some(result) = tasks.join_next().await {
        if let Err(error) = result {
            error!(%error, "connection task failed during shutdown");
        }
    }
    info!("server stopped");
    Ok(())
}

async fn handle_connection(
    mut stream: TcpStream,
    peer: SocketAddr,
    config: Config,
    _permit: OwnedSemaphorePermit,
) -> anyhow::Result<()> {
    stream.set_nodelay(true)?;
    authenticate(&mut stream, &config).await?;

    let (name, size) = match read_header(&mut stream, config.idle_timeout).await {
        Ok(header) => header,
        Err(error) => {
            reject_connection(&mut stream, config.idle_timeout).await;
            return Err(error).context("invalid or incomplete file header");
        }
    };
    if let Err(error) = validate_file_name(&name) {
        reject_connection(&mut stream, config.idle_timeout).await;
        return Err(error).context("invalid destination file name");
    }
    if size > config.max_file_size {
        reject_connection(&mut stream, config.idle_timeout).await;
        bail!(
            "file size {size} exceeds configured limit of {} bytes",
            config.max_file_size
        );
    }

    let destination = config.output_dir.join(&name);
    match fs::try_exists(&destination).await {
        Ok(true) => {
            reject_connection(&mut stream, config.idle_timeout).await;
            bail!("destination already exists: {}", destination.display());
        }
        Ok(false) => {}
        Err(error) => {
            reject_connection(&mut stream, config.idle_timeout).await;
            return Err(error).context("cannot inspect destination path");
        }
    }

    let (temp_path, mut file) = match create_temp_file(&config.output_dir).await {
        Ok(created) => created,
        Err(error) => {
            reject_connection(&mut stream, config.idle_timeout).await;
            return Err(error);
        }
    };
    if let Err(error) = send_status(&mut stream, STATUS_OK, config.idle_timeout).await {
        drop(file);
        cleanup_temp(&temp_path).await;
        return Err(error).context("failed to tell client the server is ready");
    }

    let deadline = Instant::now() + config.transfer_timeout;
    let receive_result = timeout_at(
        deadline,
        receive_contents(&mut stream, &mut file, size, config.idle_timeout),
    )
    .await;
    let receive_error = match receive_result {
        Ok(Ok(())) => None,
        Ok(Err(error)) => Some(error.context("failed while receiving file contents")),
        Err(error) => Some(anyhow::anyhow!(
            "transfer exceeded its total timeout: {error}"
        )),
    };
    drop(file);
    if let Some(error) = receive_error {
        reject_connection(&mut stream, config.idle_timeout).await;
        cleanup_temp(&temp_path).await;
        return Err(error);
    }

    if let Err(error) = fs::hard_link(&temp_path, &destination).await {
        cleanup_temp(&temp_path).await;
        reject_connection(&mut stream, config.idle_timeout).await;
        return Err(error).with_context(|| {
            format!(
                "could not publish {} without overwriting an existing file",
                destination.display()
            )
        });
    }
    if let Err(error) = fs::remove_file(&temp_path).await {
        warn!(path = %temp_path.display(), %error, "received file is complete but temporary link cleanup failed");
    }

    send_status(&mut stream, STATUS_OK, config.idle_timeout)
        .await
        .context("file was saved, but transfer confirmation could not be sent")?;
    info!(%peer, file = %name, bytes = size, "file received");
    Ok(())
}

async fn authenticate(stream: &mut TcpStream, config: &Config) -> anyhow::Result<()> {
    let auth_result = timeout(config.idle_timeout, async {
        let length = stream.read_u16().await?;
        if !(MIN_TOKEN_LENGTH..=MAX_TOKEN_LENGTH).contains(&(length as usize)) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid authentication token length",
            ));
        }
        let mut supplied = vec![0u8; length as usize];
        stream.read_exact(&mut supplied).await?;
        Ok::<_, io::Error>(supplied)
    })
    .await;

    let supplied = match auth_result {
        Ok(Ok(supplied)) => supplied,
        Ok(Err(error)) => {
            reject_with_status(stream, STATUS_AUTH_FAILED, config.idle_timeout).await;
            return Err(error).context("failed to read authentication token");
        }
        Err(error) => {
            reject_with_status(stream, STATUS_AUTH_FAILED, config.idle_timeout).await;
            return Err(error).context("authentication timed out");
        }
    };

    if !bool::from(config.token.as_ref().ct_eq(supplied.as_slice())) {
        send_status(stream, STATUS_AUTH_FAILED, config.idle_timeout).await?;
        bail!("invalid authentication token");
    }
    send_status(stream, STATUS_OK, config.idle_timeout).await?;
    Ok(())
}

async fn read_header(
    stream: &mut TcpStream,
    idle_timeout: Duration,
) -> anyhow::Result<(String, u64)> {
    let header = timeout(idle_timeout, async {
        let name_length = stream.read_u16().await? as usize;
        if name_length == 0 || name_length > MAX_NAME_LENGTH {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid file name length",
            ));
        }

        let mut name = vec![0u8; name_length];
        stream.read_exact(&mut name).await?;
        let name = String::from_utf8(name)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        let size = stream.read_u64().await?;
        Ok::<_, io::Error>((name, size))
    })
    .await
    .context("timed out waiting for file header")??;

    Ok(header)
}

async fn receive_contents(
    stream: &mut TcpStream,
    file: &mut File,
    size: u64,
    idle_timeout: Duration,
) -> anyhow::Result<()> {
    let mut remaining = size;
    let mut buffer = vec![0u8; CHUNK_SIZE];
    while remaining > 0 {
        let read_len = remaining.min(buffer.len() as u64) as usize;
        let count = timeout(idle_timeout, stream.read(&mut buffer[..read_len]))
            .await
            .context("timed out waiting for file data")??;
        if count == 0 {
            bail!("client disconnected before the declared file size was received");
        }
        file.write_all(&buffer[..count]).await?;
        remaining -= count as u64;
    }
    file.flush().await?;
    file.sync_all().await?;
    Ok(())
}

async fn create_temp_file(output_dir: &Path) -> anyhow::Result<(PathBuf, File)> {
    for _ in 0..16 {
        let id = NEXT_TEMP_ID.fetch_add(1, Ordering::Relaxed);
        let path = output_dir.join(format!(".file-transfer-{}-{id}.part", std::process::id()));
        match OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .await
        {
            Ok(file) => return Ok((path, file)),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(error) => {
                return Err(error).with_context(|| {
                    format!("cannot create temporary file in {}", output_dir.display())
                });
            }
        }
    }
    bail!("could not allocate a unique temporary file name")
}

async fn cleanup_temp(path: &Path) {
    match fs::remove_file(path).await {
        Err(error) if error.kind() != io::ErrorKind::NotFound => {
            warn!(path = %path.display(), %error, "failed to remove incomplete temporary file");
        }
        _ => {}
    }
}

async fn send_status(
    stream: &mut TcpStream,
    status: u8,
    idle_timeout: Duration,
) -> anyhow::Result<()> {
    timeout(idle_timeout, stream.write_u8(status))
        .await
        .context("timed out writing protocol status")??;
    Ok(())
}

fn reject_busy(stream: TcpStream) {
    match stream.try_write(&[STATUS_BUSY]) {
        Ok(1) => {}
        Ok(_) => warn!("could not send busy status to client"),
        Err(error) => warn!(%error, "could not report connection limit to client"),
    }
}

async fn reject_connection(stream: &mut TcpStream, idle_timeout: Duration) {
    reject_with_status(stream, STATUS_REJECTED, idle_timeout).await;
}

async fn reject_with_status(stream: &mut TcpStream, status: u8, idle_timeout: Duration) {
    if let Err(error) = send_status(stream, status, idle_timeout).await {
        warn!(%error, "could not send protocol status to client");
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

#[cfg(test)]
mod tests {
    use super::*;
    use file_transfer::{STATUS_AUTH_FAILED, encode_header};

    const TEST_TOKEN: &[u8] = b"0123456789abcdef0123456789abcdef";

    fn test_config(output_dir: PathBuf) -> Config {
        Config {
            token: Arc::from(TEST_TOKEN),
            output_dir: Arc::new(output_dir),
            max_file_size: 1024,
            idle_timeout: Duration::from_secs(2),
            transfer_timeout: Duration::from_secs(5),
        }
    }

    async fn authenticate_test_client(stream: &mut TcpStream, token: &[u8]) -> anyhow::Result<u8> {
        stream.write_u16(token.len() as u16).await?;
        stream.write_all(token).await?;
        Ok(stream.read_u8().await?)
    }

    async fn connect_test_client(address: SocketAddr, token: &[u8]) -> anyhow::Result<TcpStream> {
        let mut stream = TcpStream::connect(address).await?;
        anyhow::ensure!(
            authenticate_test_client(&mut stream, token).await? == STATUS_OK,
            "authentication failed"
        );
        Ok(stream)
    }

    async fn send_test_header(stream: &mut TcpStream, name: &str, size: u64) -> anyhow::Result<u8> {
        stream.write_all(&encode_header(name, size)?).await?;
        Ok(stream.read_u8().await?)
    }

    #[tokio::test]
    async fn authenticated_upload_is_saved_and_limits_are_enforced() -> anyhow::Result<()> {
        let output_dir = std::env::temp_dir().join(format!(
            "file-transfer-test-{}-{}",
            std::process::id(),
            NEXT_TEMP_ID.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&output_dir).await?;

        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let config = test_config(output_dir.clone());
        let server = tokio::spawn(async move {
            for _ in 0..4 {
                let (stream, peer) = listener.accept().await?;
                let permit = Arc::new(Semaphore::new(1)).acquire_owned().await?;
                let _ = handle_connection(stream, peer, config.clone(), permit).await;
            }
            anyhow::Ok(())
        });

        let contents = b"tokio file transfer";
        let mut stream = connect_test_client(address, TEST_TOKEN).await?;
        assert_eq!(
            send_test_header(&mut stream, "sample.bin", contents.len() as u64).await?,
            STATUS_OK
        );
        stream.write_all(contents).await?;
        assert_eq!(stream.read_u8().await?, STATUS_OK);
        assert_eq!(fs::read(output_dir.join("sample.bin")).await?, contents);

        let mut unauthorized = TcpStream::connect(address).await?;
        assert_eq!(
            authenticate_test_client(&mut unauthorized, b"fedcba9876543210fedcba9876543210")
                .await?,
            STATUS_AUTH_FAILED
        );

        let mut duplicate = connect_test_client(address, TEST_TOKEN).await?;
        assert_eq!(
            send_test_header(&mut duplicate, "sample.bin", 1).await?,
            STATUS_REJECTED
        );
        assert_eq!(fs::read(output_dir.join("sample.bin")).await?, contents);

        let mut oversized = connect_test_client(address, TEST_TOKEN).await?;
        assert_eq!(
            send_test_header(&mut oversized, "large.bin", 1025).await?,
            STATUS_REJECTED
        );
        assert!(!fs::try_exists(output_dir.join("large.bin")).await?);

        server.await??;
        fs::remove_dir_all(output_dir).await?;
        Ok(())
    }
}
