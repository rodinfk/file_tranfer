use std::io;

pub const MAX_NAME_LENGTH: usize = 255;
pub const MAX_TOKEN_LENGTH: usize = 128;
pub const MIN_TOKEN_LENGTH: usize = 16;
pub const DEFAULT_MAX_FILE_SIZE: u64 = 1024 * 1024 * 1024;
pub const DEFAULT_MAX_CONNECTIONS: usize = 32;
pub const DEFAULT_IDLE_TIMEOUT_SECS: u64 = 30;
pub const DEFAULT_TRANSFER_TIMEOUT_SECS: u64 = 3600;
pub const STATUS_OK: u8 = 0;
pub const STATUS_AUTH_FAILED: u8 = 1;
pub const STATUS_BUSY: u8 = 2;
pub const STATUS_REJECTED: u8 = 3;

pub fn encode_header(name: &str, size: u64) -> io::Result<Vec<u8>> {
    let name = name.as_bytes();
    if name.is_empty() || name.len() > MAX_NAME_LENGTH {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "file name length must be between 1 and 255 bytes",
        ));
    }

    let mut header = Vec::with_capacity(2 + name.len() + 8);
    header.extend_from_slice(&(name.len() as u16).to_be_bytes());
    header.extend_from_slice(name);
    header.extend_from_slice(&size.to_be_bytes());

    Ok(header)
}

pub fn parse_header(buffer: &[u8]) -> io::Result<Option<(String, u64, usize)>> {
    if buffer.len() < 2 {
        return Ok(None);
    }

    let name_length = u16::from_be_bytes([buffer[0], buffer[1]]) as usize;
    if name_length == 0 || name_length > MAX_NAME_LENGTH {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid file name length",
        ));
    }

    let header_length = 2 + name_length + 8;
    if buffer.len() < header_length {
        return Ok(None);
    }

    let name = std::str::from_utf8(&buffer[2..2 + name_length])
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?
        .to_owned();
    let size_offset = 2 + name_length;
    let size = u64::from_be_bytes(
        buffer[size_offset..header_length]
            .try_into()
            .expect("header length was checked"),
    );

    Ok(Some((name, size, header_length)))
}

pub fn validate_file_name(name: &str) -> io::Result<()> {
    if name.is_empty()
        || name.len() > MAX_NAME_LENGTH
        || name == "."
        || name == ".."
        || name.ends_with([' ', '.'])
        || name.chars().any(|character| {
            character.is_control()
                || matches!(
                    character,
                    '/' | '\\' | ':' | '<' | '>' | '"' | '|' | '?' | '*'
                )
        })
        || is_reserved_windows_name(name)
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "file name is empty, too long, or contains invalid characters",
        ));
    }

    Ok(())
}

fn is_reserved_windows_name(name: &str) -> bool {
    let base = name
        .split('.')
        .next()
        .unwrap_or(name)
        .trim_end_matches([' ', '.'])
        .to_ascii_uppercase();
    matches!(
        base.as_str(),
        "CON"
            | "PRN"
            | "AUX"
            | "NUL"
            | "COM1"
            | "COM2"
            | "COM3"
            | "COM4"
            | "COM5"
            | "COM6"
            | "COM7"
            | "COM8"
            | "COM9"
            | "LPT1"
            | "LPT2"
            | "LPT3"
            | "LPT4"
            | "LPT5"
            | "LPT6"
            | "LPT7"
            | "LPT8"
            | "LPT9"
    )
}

#[cfg(test)]
mod tests {
    use super::{encode_header, parse_header, validate_file_name};

    #[test]
    fn title_round_trips() {
        let encoded = encode_header("report.bin", 1234).unwrap();
        assert_eq!(
            parse_header(&encoded).unwrap(),
            Some(("report.bin".to_owned(), 1234, encoded.len()))
        );
    }

    #[test]
    fn incomplete_title_is_buffered() {
        let encoded = encode_header("x", 7).unwrap();
        assert_eq!(parse_header(&encoded[..encoded.len() - 1]).unwrap(), None);
    }

    #[test]
    fn invalid_title_is_rejected() {
        assert!(parse_header(&[0, 0]).is_err());
        assert!(parse_header(&[0, 1, 0xff, 0, 0, 0, 0, 0, 0, 0, 0]).is_err());
    }

    #[test]
    fn file_name_validation_rejects_paths() {
        assert!(validate_file_name("report.txt").is_ok());
        assert!(validate_file_name("../report.txt").is_err());
        assert!(validate_file_name("folder\\report.txt").is_err());
        assert!(validate_file_name("C:report.txt").is_err());
        assert!(validate_file_name("CON.txt").is_err());
        assert!(validate_file_name("report.txt.").is_err());
        assert!(validate_file_name("").is_err());
    }
}
