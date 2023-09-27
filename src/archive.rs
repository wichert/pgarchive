use crate::io::ReadConfig;
use crate::toc::{read_toc, TocEntry};
use crate::types::{ArchiveError, CompressionMethod, Offset, Section, Version};
use chrono::prelude::*;
use std::fmt;
use std::fs::File;
use std::io;
use std::string::String;

const MIN_SUPPORTED_VERSION: Version = (1, 10, 0);
const MAX_SUPPORTED_VERSION: Version = (1, 15, 0);

#[derive(Debug, PartialEq)]
pub struct Archive {
    pub version: Version,
    pub compression_method: CompressionMethod,
    pub compression_level: i64,
    pub create_date: NaiveDateTime,
    pub database_name: String,
    pub server_version: String,
    pub pgdump_version: String,
    pub toc_entries: Vec<TocEntry>,
    io_config: ReadConfig,
}

impl fmt::Display for Archive {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "version={}.{}.{} compression={}",
            self.version.0, self.version.1, self.version.2, self.compression_method
        )
    }
}

impl Archive {
    pub fn parse(f: &mut (impl io::Read + ?Sized)) -> Result<Archive, ArchiveError> {
        let mut buffer = vec![0; 5];
        f.read_exact(buffer.as_mut_slice())?;
        if buffer != "PGDMP".as_bytes() {
            return Err(ArchiveError::InvalidData);
        }

        let mut io_config = ReadConfig::new();
        let version: Version = (
            io_config.read_byte(f)?,
            io_config.read_byte(f)?,
            io_config.read_byte(f)?,
        );

        if version < MIN_SUPPORTED_VERSION || version > MAX_SUPPORTED_VERSION {
            return Err(ArchiveError::UnsupportedVersionError(version));
        }

        io_config.int_size = io_config.read_byte(f)? as usize;
        io_config.offset_size = io_config.read_byte(f)? as usize;

        if io_config.read_byte(f)? != 1 {
            return Err(ArchiveError::IOError(io::Error::new(
                io::ErrorKind::Other,
                "wrong file format",
            )));
        }

        let mut compression_method = CompressionMethod::None;
        let mut compression_level = 0;

        if version >= (1, 15, 0) {
            compression_method = io_config
                .read_byte(f)?
                .try_into()
                .or(Err(ArchiveError::InvalidData))?;
        } else {
            compression_level = io_config.read_int(f)?;
            if compression_level != 0 {
                compression_method = CompressionMethod::Gzip;
            }
        }

        let created_sec = io_config.read_int(f)?;
        let created_min = io_config.read_int(f)?;
        let created_hour = io_config.read_int(f)?;
        let created_mday = io_config.read_int(f)?;
        let created_mon = io_config.read_int(f)?;
        let created_year = io_config.read_int(f)?;
        let _created_isdst = io_config.read_int(f)?;

        let create_date = NaiveDate::from_ymd_opt(
            (created_year + 1900) as i32,
            created_mon as u32,
            created_mday as u32,
        )
        .ok_or(ArchiveError::InvalidData)?
        .and_hms_opt(created_hour as u32, created_min as u32, created_sec as u32)
        .ok_or(ArchiveError::InvalidData)?;

        let database_name = io_config.read_string(f)?;
        let server_version = io_config.read_string(f)?;
        let pgdump_version = io_config.read_string(f)?;
        let toc_entries = read_toc(f, &io_config)?;

        Ok(Archive {
            version,
            compression_method,
            compression_level,
            create_date,
            database_name,
            server_version,
            pgdump_version,
            toc_entries,
            io_config,
        })
    }

    pub fn get_toc_entry(&self, section: Section, tag: &str) -> Option<&TocEntry> {
        self.toc_entries
            .iter()
            .find(|e| e.section == section && e.tag == tag)
    }

    pub fn read_data(&self, f: &mut File, o: Offset) -> Result<Box<dyn io::Read>, ArchiveError> {
        self.io_config.read_data(f, o)
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
            "00 00 00 00 00" // toc size
        )[..];

        let header = Archive::parse(&mut input)?;
        assert_eq!(
            header,
            Archive {
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
                toc_entries: vec![],
                io_config: ReadConfig {
                    int_size: 4,
                    offset_size: 8
                }
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
            "00 00 00 00 00" // toc size
        )[..];

        let header = Archive::parse(&mut input)?;
        assert_eq!(
            header,
            Archive {
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
                toc_entries: vec![],
                io_config: ReadConfig {
                    int_size: 4,
                    offset_size: 8
                }
            }
        );
        Ok(())
    }
}
