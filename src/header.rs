use crate::io::ReadConfig;
use chrono::prelude::*;
use std::fmt;
use std::io;
use std::string::String;

pub type Version = (u8, u8, u8);

const MIN_SUPPORTED_VERSION: Version = (1, 15, 0);

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

#[derive(Debug)]
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

#[derive(Debug)]
pub struct Header {
    pub version: Version,
    pub compression: CompressionMethod,
    pub create_date: NaiveDateTime,
    pub connection: String,
    pub database_name: String,
    pub server_version: String,
    pub pgdump_version: String,
}

impl fmt::Display for Header {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "version={}.{}.{} compression={}",
            self.version.0, self.version.1, self.version.2, self.compression
        )
    }
}

impl TryFrom<&mut dyn io::Read> for Header {
    type Error = ArchiveError;

    fn try_from(f: &mut dyn io::Read) -> Result<Self, Self::Error> {
        let mut header = Header {
            version: (0, 0, 0),
            compression: CompressionMethod::None,
            create_date: NaiveDateTime::MIN,
            connection: "".to_string(),
            database_name: "".to_string(),
            server_version: "".to_string(),
            pgdump_version: "".to_string(),
        };

        let mut buffer = Vec::with_capacity(51);
        buffer.resize(51, 0);
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

        if header.version < MIN_SUPPORTED_VERSION {
            return Err(ArchiveError::UnsupportedVersionError(header.version));
        }

        if cfg.read_byte(f)? != 1 {
            return Err(ArchiveError::IOError(io::Error::new(
                io::ErrorKind::Other,
                "wrong file format",
            )));
        }

        let compression = cfg.read_byte(f)?;
        match compression {
            0 => header.compression = CompressionMethod::None,
            1 => header.compression = CompressionMethod::Gzip,
            2 => header.compression = CompressionMethod::LZ4,
            3 => header.compression = CompressionMethod::ZSTD,
            _ => return Err(ArchiveError::InvalidData),
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
