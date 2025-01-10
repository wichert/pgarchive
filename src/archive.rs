use crate::io::ReadConfig;
use crate::toc::{read_toc, TocEntry};
use crate::types::{ArchiveError, CompressionMethod, Section, Version};
use chrono::prelude::*;
use flate2::read::GzDecoder;
use flate2::read::ZlibDecoder;
use std::fmt;
use std::fs::File;
use std::io;
use std::string::String;

// Historical version numbers are described in `postgres/src/bin/pg_dump/pg_backup_archiver.h`

/// PostgreSQL 8.0 - add tablespace.
pub const K_VERS_1_10: Version = (1, 10, 0);

/// PostgreSQL 8.4 - add toc section indicator.
pub const K_VERS_1_11: Version = (1, 11, 0);

/// PostgreSQL 9.0 - add separate BLOB entries.
#[allow(dead_code)]
pub const K_VERS_1_12: Version = (1, 12, 0);

/// PostgreSQL 11 - change search_path behavior.
#[allow(dead_code)]
pub const K_VERS_1_13: Version = (1, 13, 0);

/// PostgreSQL 12 - add tableam.
pub const K_VERS_1_14: Version = (1, 14, 0);

/// PostgreSQL 16 - add compression_algorithm in header.
pub const K_VERS_1_15: Version = (1, 15, 0);

/// PostgreSQL 17 - BLOB METADATA entries and multiple BLOBS, relkind.
pub const K_VERS_1_16: Version = (1, 16, 0);

/// An object providing access to a PostgreSQL archive
///
/// `Archive` instances should be created using `Archive::parse`, which will parse
/// the file header and return an initialized `Archive` instance.
///
/// # Example
///
/// ```rust
/// use std::fs::File;
/// use pgarchive::Archive;
///
/// let mut file = File::open("tests/test.pgdump").unwrap();
/// match Archive::parse(&mut file) {
///     Ok(archive) => println!("This is a backup of {}", archive.database_name),
///     Err(e) => println!("can not read file: {:?}", e),
/// };
/// ```

#[derive(Debug, PartialEq)]
pub struct Archive {
    /// Archive format version.
    ///
    /// This is generally aligned with the PostgreSQL version, but only updated
    /// when the file format changes.
    pub version: Version,

    /// Compression method used for data and blobs
    pub compression_method: CompressionMethod,

    /// Date when the archive was created
    pub create_date: NaiveDateTime,

    /// Name of the database that was dumped
    pub database_name: String,

    /// Version information for PostgreSQL server that pg_dump was accessing.
    ///
    /// The format of this string differs per PostgreSQL version. An
    /// example value is `14.6 (Homebrew)`.
    pub server_version: String,

    /// Version information for pg_dump command that was used to create the archive.
    ///
    /// The format of this string differs per PostgreSQL version. An
    /// example value is `14.6 (Homebrew)`.
    pub pgdump_version: String,

    /// The table of contents for the archive.
    ///
    /// This is a list of all entities in the archive.
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
    /// Read and parse the archive header.
    ///
    /// This function reads the archive header from a file-like object, and returns
    /// a new `Archive` instance.
    pub fn parse(f: &mut (impl io::Read + ?Sized)) -> Result<Archive, ArchiveError> {
        let mut buffer = vec![0; 5];
        f.read_exact(buffer.as_mut_slice())?;
        if buffer != "PGDMP".as_bytes() {
            return Err(ArchiveError::InvalidData(
                "file does not start with PGDMP".into(),
            ));
        }

        let mut io_config = ReadConfig::new();
        let version: Version = (
            io_config.read_byte(f)?,
            io_config.read_byte(f)?,
            io_config.read_byte(f)?,
        );

        if version < K_VERS_1_10 || version > K_VERS_1_16 {
            return Err(ArchiveError::UnsupportedVersionError(version));
        }

        io_config.int_size = io_config.read_byte(f)? as usize;
        io_config.offset_size = io_config.read_byte(f)? as usize;

        if io_config.read_byte(f)? != 1 {
            // 1 = archCustom
            return Err(ArchiveError::InvalidData(
                "file format must be 1 (custom)".into(),
            ));
        }

        let compression_method = if version >= K_VERS_1_15 {
            io_config
                .read_byte(f)?
                .try_into()
                .or(Err(ArchiveError::InvalidData(
                    "invalid compression method".into(),
                )))?
        } else {
            let compression = io_config.read_int(f)?;
            match compression {
                -1 => Ok(CompressionMethod::ZSTD),
                0 => Ok(CompressionMethod::None),
                1..=9 => Ok(CompressionMethod::Gzip(compression)),
                _ => Err(ArchiveError::InvalidData(
                    "invalid compression method".into(),
                )),
            }?
        };

        let created_sec = io_config.read_int(f)?;
        let created_min = io_config.read_int(f)?;
        let created_hour = io_config.read_int(f)?;
        let created_mday = io_config.read_int(f)?;
        let created_mon = io_config.read_int(f)?;
        let created_year = io_config.read_int(f)?;
        let _created_isdst = io_config.read_int(f)?;

        let create_date = NaiveDate::from_ymd_opt(
            (created_year + 1900) as i32,
            (created_mon + 1) as u32,
            created_mday as u32,
        )
        .ok_or(ArchiveError::InvalidData("invalid creation date".into()))?
        .and_hms_opt(created_hour as u32, created_min as u32, created_sec as u32)
        .ok_or(ArchiveError::InvalidData(
            "invalid time in creation date".into(),
        ))?;

        let database_name = io_config.read_string(f)?;
        let server_version = io_config.read_string(f)?;
        let pgdump_version = io_config.read_string(f)?;
        let toc_entries = read_toc(f, &io_config, version)?;

        Ok(Archive {
            version,
            compression_method,
            create_date,
            database_name,
            server_version,
            pgdump_version,
            toc_entries,
            io_config,
        })
    }

    /// Find a TOC entry by name and section.
    ///
    /// This function provides a simple method to find a TOC entry, so you
    /// do not need to iterate over `toc_entries`.
    ///
    /// ```rust
    /// # use std::fs::File;
    /// # use pgarchive::Archive;
    /// # let mut file = File::open("tests/test.pgdump").unwrap();
    /// # let archive = Archive::parse(&mut file).unwrap();
    /// let employee_toc = archive.find_toc_entry(pgarchive::Section::Data, "TABLE DATA", "employee");
    /// ```
    pub fn find_toc_entry(&self, section: Section, desc: &str, tag: &str) -> Option<&TocEntry> {
        self.toc_entries
            .iter()
            .find(|e| e.section == section && e.desc == desc && e.tag == tag)
    }

    /// Access data for a TOC entry.
    ///
    /// This function provides access to the data for a TOC entry. This is only
    /// applicable to entries in the `Section::Data` section.
    ///
    /// Decompression is automatically handled, so you can read the data directly
    /// from the returned [`Read`](io::Read) instance.
    ///
    /// # Example
    ///
    /// ```rust
    /// # use std::fs::File;
    /// # use pgarchive::Archive;
    /// # fn main() -> Result<(), pgarchive::ArchiveError> {
    /// # let mut file = File::open("tests/test.pgdump").unwrap();
    /// # let archive = Archive::parse(&mut file).unwrap();
    /// let employee_toc = archive
    ///         .find_toc_entry(pgarchive::Section::Data, "TABLE DATA", "pizza")
    ///         .expect("no data for pizza table present");
    /// let mut data = archive.read_data(&mut file, &employee_toc)?;
    /// let mut buffer = Vec::new();
    /// let size = data.read_to_end(&mut buffer)?;
    /// println!("the pizza table data has {} bytes of data", size);
    /// #     Ok(())
    /// # }
    /// ```
    pub fn read_data(
        &self,
        f: &mut File,
        entry: &TocEntry,
    ) -> Result<Box<dyn io::Read>, ArchiveError> {
        let reader = self.io_config.read_data(f, entry.offset)?;
        match self.compression_method {
            CompressionMethod::None => Ok(reader),
            CompressionMethod::ZSTD => Ok(Box::new(ZlibDecoder::new(reader))),
            CompressionMethod::Gzip(_) => Ok(Box::new(GzDecoder::new(reader))),
            _ => Err(ArchiveError::CompressionMethodNotSupported(
                self.compression_method,
            )),
        }
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
                compression_method: CompressionMethod::ZSTD,
                create_date: NaiveDate::from_ymd_opt(2022, 11, 24)
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
                create_date: NaiveDate::from_ymd_opt(2022, 11, 24)
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
    fn header_create_date_with_zero_indexed_month() -> Result<(), ArchiveError> {
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
        "00 00 00 00 00" // Months (0-indexed, so this is January)
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
                compression_method: CompressionMethod::ZSTD,
                create_date: NaiveDate::from_ymd_opt(2022, 1, 24)
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
