use std::fmt;
use std::io;
use thiserror::Error;

/// Type used for PostgreSQL version numbers
pub type Version = (u8, u8, u8);

/// Error type used for archive processing errors.
///
/// Errors can be caused by underlying IO errors, unsupported features or
/// invalid data.
#[derive(Error, Debug)]
pub enum ArchiveError {
    /// An IO errors occured while reading data.
    #[error("IO error reading data")]
    IOError(#[from] io::Error),
    /// Invalid data was found. This should only happen if the archive is
    /// corrupted (or pgarchive has a bug).
    #[error("format error: {0}")]
    InvalidData(String),
    /// Invalid TocEntry data was found. This should only happen if the archive is
    /// corrupted (or pgarchive has a bug).
    #[error("format error for id {0}: {1}")]
    InvalidEntryData(crate::toc::ID, String),
    /// Returned when you try to read the data for a
    /// [`TocEntry`](crate::TocEntry), but it has no data.
    #[error("TOC entry has no data")]
    NoDataPresent,
    /// pgarchive does not support reading blob data.
    #[error("reading BLOB data is not supported")]
    BlobNotSupported,
    /// The archive was made by a pg_dump version that is not supported by this
    /// crate.
    #[error("archive format {}.{}.{} is not supported", (.0).0, (.0).1, (.0).2)]
    UnsupportedVersionError(Version),
    /// An unsupported compression method was used for table data.
    #[error("compression method {0} is not supported")]
    CompressionMethodNotSupported(CompressionMethod),
}

pub type Oid = u64;

#[derive(Clone, Copy, PartialEq, Debug)]
pub enum Offset {
    Unknown,
    PosNotSet,
    PosSet(u64),
    NoData,
}

#[derive(Clone, Copy, Debug, PartialEq)]
#[repr(u8)]
pub enum BlockType {
    Data = 1,
    Blob = 3,
}

impl TryFrom<u8> for BlockType {
    type Error = ();

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            x if x == BlockType::Data as u8 => Ok(BlockType::Data),
            x if x == BlockType::Blob as u8 => Ok(BlockType::Blob),
            _ => Err(()),
        }
    }
}

/// Possible compression methods used for data.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum CompressionMethod {
    /// Data is not compressed
    None,
    /// Data is compressed using gzip, with the given compress level (1..9)
    Gzip(i64),
    /// Data is compressed using [LZ4](https://lz4.org).
    LZ4,
    /// Data is compressed using DEFLATE.
    ZSTD,
}

impl TryFrom<u8> for CompressionMethod {
    type Error = ();

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(CompressionMethod::None),
            1 => Ok(CompressionMethod::Gzip(0)),
            2 => Ok(CompressionMethod::LZ4),
            3 => Ok(CompressionMethod::ZSTD),
            _ => Err(()),
        }
    }
}

impl fmt::Display for CompressionMethod {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

/// Enumeration of table of contents section types.
///
/// Each entry in the table of contents is associate with a section, which
/// determines the order in which the entries are processed during a restore.
/// The order is:
///
/// 1. PreData
/// 1. Data
/// 1. PostData
#[derive(Clone, Copy, Debug, PartialEq)]
#[repr(u8)]
pub enum Section {
    /// Used for table of contents entries that do not modify the schema or add
    /// data.
    None = 1,
    /// Indicates an entry that must be processed before table data is loaded. This
    /// is normally used for creation of schemas, tables, setting the search path, etc.
    PreData,
    /// Used for entries that load data into tables.
    Data,
    /// Used for entries that must be processed after table data is loaded. This
    /// is used for things like setting the next value of sequences.
    PostData,
}

impl TryFrom<i64> for Section {
    type Error = ();

    fn try_from(value: i64) -> Result<Self, Self::Error> {
        match value {
            x if x == Section::None as i64 => Ok(Section::None),
            x if x == Section::PreData as i64 => Ok(Section::PreData),
            x if x == Section::Data as i64 => Ok(Section::Data),
            x if x == Section::PostData as i64 => Ok(Section::PostData),
            _ => Err(()),
        }
    }
}

impl fmt::Display for Section {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}
