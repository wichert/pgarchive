use std::fmt;
use std::io;

pub type Version = (u8, u8, u8);

#[derive(Debug)]
pub enum ArchiveError {
    IOError(io::Error),
    InvalidData,
    NoDataPresent,
    BlobNotSupported,
    UnsupportedVersionError(Version),
    CompressionMethodNotSupported(CompressionMethod),
}

impl From<io::Error> for ArchiveError {
    fn from(e: io::Error) -> ArchiveError {
        ArchiveError::IOError(e)
    }
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

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum CompressionMethod {
    None,
    Gzip(i64),
    LZ4,
    ZSTD,
}

impl TryFrom<u8> for CompressionMethod {
    type Error = ();

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            x if x == 0 => Ok(CompressionMethod::None),
            x if x == 1 => Ok(CompressionMethod::Gzip(0)),
            x if x == 2 => Ok(CompressionMethod::LZ4),
            x if x == 3 => Ok(CompressionMethod::ZSTD),
            _ => Err(()),
        }
    }
}

impl fmt::Display for CompressionMethod {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
#[repr(u8)]
pub enum Section {
    None = 1,
    PreData,
    Data,
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
