//! Parser for PostgreSQL dumps in custom format
//!
//! This crate allows inspecting the contents of a PostgreSQL backup
//! as made using `pg_dump -Fc` or `pg_dump --format=custom`, and provides
//! direct access all raw table data. This can be useful if you do not
//! trust the SQL statements embedded in the dump, or if you want to
//! process data without loading it into a database.
//!
//! ```rust
//! use std::fs::File;
//! use pgarchive::Archive;
//!
//! fn main() {
//!     let mut file = File::open("tests/test.pgdump").unwrap();
//!     match Archive::parse(&mut file) {
//!         Ok(archive) => println!("This is a backup of {}", archive.database_name),
//!         Err(e) => println!("can not read file: {:?}", e),
//!     };
//! }
//! ```
mod archive;
mod io;
mod toc;
mod types;

pub use archive::Archive;
pub use toc::{TocEntry, ID};
pub use types::{ArchiveError, CompressionMethod, Section, Version};
