use crate::io::ReadConfig;
use chrono::prelude::*;
use std::fmt;
use std::io;
use std::string::String;

pub type Version = (u8, u8, u8);

const MIN_SUPPORTED_VERSION: Version = (1, 10, 0);
const MAX_SUPPORTED_VERSION: Version = (1, 15, 0);

#[derive(Debug)]
pub enum ArchiveError {
    IOError(io::Error),
    InvalidData,
    UnsupportedVersionError(Version),
}

impl From<io::Error> for ArchiveError {
    fn from(e: io::Error) -> ArchiveError {
        ArchiveError::IOError(e)
    }
}

#[derive(Debug, PartialEq)]
pub enum CompressionMethod {
    None,
    Gzip,
    LZ4,
    ZSTD,
}

impl fmt::Display for CompressionMethod {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[derive(Debug, PartialEq)]
pub struct Header {
    pub version: Version,
    pub compression_method: CompressionMethod,
    pub compression_level: i64,
    pub create_date: NaiveDateTime,
    pub database_name: String,
    pub server_version: String,
    pub pgdump_version: String,
}

impl fmt::Display for Header {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "version={}.{}.{} compression={}",
            self.version.0, self.version.1, self.version.2, self.compression_method
        )
    }
}

impl Header {
    pub fn parse(f: &mut (impl io::Read + ?Sized)) -> Result<Header, ArchiveError> {
        let mut header = Header {
            version: (0, 0, 0),
            compression_method: CompressionMethod::None,
            compression_level: 0,
            create_date: NaiveDateTime::MIN,
            database_name: String::from(""),
            server_version: String::from(""),
            pgdump_version: String::from(""),
        };

        let mut buffer = Vec::with_capacity(5);
        buffer.resize(5, 0);
        f.read_exact(buffer.as_mut_slice())?;
        if buffer != "PGDMP".as_bytes() {
            return Err(ArchiveError::InvalidData);
        }

        let mut cfg = ReadConfig::new();
        header.version.0 = cfg.read_byte(f)?;
        header.version.1 = cfg.read_byte(f)?;
        header.version.2 = cfg.read_byte(f)?;
        cfg.int_size = cfg.read_byte(f)? as usize;
        cfg.offset_size = cfg.read_byte(f)? as usize;

        if header.version < MIN_SUPPORTED_VERSION || header.version > MAX_SUPPORTED_VERSION {
            return Err(ArchiveError::UnsupportedVersionError(header.version));
        }

        if cfg.read_byte(f)? != 1 {
            return Err(ArchiveError::IOError(io::Error::new(
                io::ErrorKind::Other,
                "wrong file format",
            )));
        }

        if header.version >= (1, 15, 0) {
            match cfg.read_byte(f)? {
                0 => header.compression_method = CompressionMethod::None,
                1 => header.compression_method = CompressionMethod::Gzip,
                2 => header.compression_method = CompressionMethod::LZ4,
                3 => header.compression_method = CompressionMethod::ZSTD,
                _ => return Err(ArchiveError::InvalidData),
            }
        } else {
            header.compression_level = cfg.read_int(f)?;
            if header.compression_level != 0 {
                header.compression_method = CompressionMethod::Gzip;
            }
        }

        let created_sec = cfg.read_int(f)?;
        let created_min = cfg.read_int(f)?;
        let created_hour = cfg.read_int(f)?;
        let created_mday = cfg.read_int(f)?;
        let created_mon = cfg.read_int(f)?;
        let created_year = cfg.read_int(f)?;
        let _created_isdst = cfg.read_int(f)?;

        header.create_date = NaiveDate::from_ymd_opt(
            (created_year + 1900) as i32,
            created_mon as u32,
            created_mday as u32,
        )
        .ok_or(ArchiveError::InvalidData)?
        .and_hms_opt(created_hour as u32, created_min as u32, created_sec as u32)
        .ok_or(ArchiveError::InvalidData)?;

        header.database_name = cfg.read_string(f)?;
        header.server_version = cfg.read_string(f)?;
        header.pgdump_version = cfg.read_string(f)?;

        Ok(header)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hex_literal::hex;

    #[test]
    fn v14_header() -> Result<(), ArchiveError> {
        let mut input = &hex!(
            "50 47 44 4d 50" // PGDMP
            "01 0e 00"  // major, minor, patch version
            "04" // integer size
            "08" // offset size
            "01" // header format
            "01 01 00 00 00" // Compression level
            "00 14 00 00 00" // Seconds
            "00 35 00 00 00" // Minutes
            "00 07 00 00 00" // Hours
            "00 18 00 00 00" // Days
            "00 0a 00 00 00" // Months
            "00 7a 00 00 00" // Years (since 1900)
            "00 00 00 00 00" // is DST
            "00 07 00 00 00 77 69 63 68 65 72 74" // database name
            "00 0f 00 00 00 31 34 2e 36 20 28 48 6f 6d 65 62 72 65 77 29" // server version
            "00 0f 00 00 00 31 34 2e 36 20 28 48 6f 6d 65 62 72 65 77 29" // pg_dump version
        )[..];

        let header = Header::parse(&mut input)?;
        assert_eq!(
            header,
            Header {
                version: (1, 14, 0),
                compression_method: CompressionMethod::Gzip,
                compression_level: -1,
                create_date: NaiveDate::from_ymd_opt(2022, 10, 24)
                    .unwrap()
                    .and_hms_opt(7, 53, 20)
                    .unwrap(),
                database_name: String::from("wichert"),
                server_version: String::from("14.6 (Homebrew)"),
                pgdump_version: String::from("14.6 (Homebrew)"),
            }
        );
        Ok(())
    }

    #[test]
    fn v15_header() -> Result<(), ArchiveError> {
        let mut input = &hex!(
            "50 47 44 4d 50" // PGDMP
            "01 0f 00"  // major, minor, patch version
            "04" // integer size
            "08" // offset size
            "01" // header format
            "02" // Compression method (LZ4)
            "00 14 00 00 00" // Seconds
            "00 35 00 00 00" // Minutes
            "00 07 00 00 00" // Hours
            "00 18 00 00 00" // Days
            "00 0a 00 00 00" // Months
            "00 7a 00 00 00" // Years (since 1900)
            "00 00 00 00 00" // is DST
            "00 07 00 00 00 77 69 63 68 65 72 74" // database name
            "00 0f 00 00 00 31 34 2e 36 20 28 48 6f 6d 65 62 72 65 77 29" // server version
            "00 0f 00 00 00 31 34 2e 36 20 28 48 6f 6d 65 62 72 65 77 29" // pg_dump version
        )[..];

        let header = Header::parse(&mut input)?;
        assert_eq!(
            header,
            Header {
                version: (1, 15, 0),
                compression_method: CompressionMethod::LZ4,
                compression_level: 0,
                create_date: NaiveDate::from_ymd_opt(2022, 10, 24)
                    .unwrap()
                    .and_hms_opt(7, 53, 20)
                    .unwrap(),
                database_name: String::from("wichert"),
                server_version: String::from("14.6 (Homebrew)"),
                pgdump_version: String::from("14.6 (Homebrew)"),
            }
        );
        Ok(())
    }
}
