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
}

impl From<io::Error> for ArchiveError {
    fn from(e: io::Error) -> ArchiveError {
        ArchiveError::IOError(e)
    }
}

pub type Oid = u64;

#[derive(PartialEq, Debug)]
pub enum Offset {
    Unknown,
    PosNotSet,
    PosSet(u64),
    NoData,
}

#[derive(Debug, PartialEq)]
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

#[derive(Debug, PartialEq)]
#[repr(u8)]
pub enum CompressionMethod {
    None = 0,
    Gzip,
    LZ4,
    ZSTD,
}

impl TryFrom<u8> for CompressionMethod {
    type Error = ();

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            x if x == CompressionMethod::None as u8 => Ok(CompressionMethod::None),
            x if x == CompressionMethod::Gzip as u8 => Ok(CompressionMethod::Gzip),
            x if x == CompressionMethod::LZ4 as u8 => Ok(CompressionMethod::LZ4),
            x if x == CompressionMethod::ZSTD as u8 => Ok(CompressionMethod::ZSTD),
            _ => Err(()),
        }
    }
}

impl fmt::Display for CompressionMethod {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}
