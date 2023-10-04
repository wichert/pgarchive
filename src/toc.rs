use crate::io::ReadConfig;
use crate::types::{ArchiveError, Offset, Oid, Section};
use std::io::prelude::*;

/// Type used for object identifiers
pub type ID = i64;

/// Object containing the data for a TOC entry.
///
/// All data in an archive is specific in the [table of
/// contents](crate::archive::Archive::toc_entries). The TOC entry contains all
/// metadata, including the SQL statements to create and destroy database
/// elements.
#[derive(Debug, PartialEq)]
pub struct TocEntry {
    pub id: ID,
    pub had_dumper: bool,
    pub table_oid: u64,
    pub oid: Oid,
    /// Name of object that is created or modified.
    pub tag: String,
    /// Type of object that is created or modified.
    ///
    /// For example `DATABASE`, `SEQUENCE` or `TABLE DATA`.
    pub desc: String,
    pub section: Section,
    /// SQL statement to create the database object or change a setting.
    pub defn: String,
    /// SQL statement to destroy the database object.
    pub drop_stmt: String,
    pub copy_stmt: String,
    /// PostgreSQL schema in which the object is located
    pub namespace: String,
    pub tablespace: String,
    pub table_access_method: String,
    /// PostgreSQL user that owns the object.
    pub owner: String,
    /// List of TOC entries that must be created first.
    pub dependencies: Vec<ID>,
    /// File offset where data or blob content is stored.
    pub offset: Offset,
}

impl TocEntry {
    /// Read and parse a TOC entry from a file.
    ///
    /// This function is used by [`Archive::parse`](crate::archive::Archive::parse),
    /// and should not ne called directly.
    pub fn parse(f: &mut (impl Read + ?Sized), cfg: &ReadConfig) -> Result<TocEntry, ArchiveError> {
        let id: ID = cfg.read_int(f)?;
        if id < 0 {
            return Err(ArchiveError::InvalidData("negative TOC id".into()));
        }
        let had_dumper = cfg.read_int_bool(f)?;
        let table_oid = cfg.read_oid(f)?;
        let oid = cfg.read_oid(f)?;
        let tag = cfg.read_string(f)?;
        let desc = cfg.read_string(f)?;
        let section: Section = cfg
            .read_int(f)?
            .try_into()
            .or(Err(ArchiveError::InvalidData(
                "invalid section type".into(),
            )))?;
        let defn = cfg.read_string(f)?;
        let drop_stmt = cfg.read_string(f)?;
        let copy_stmt = cfg.read_string(f)?;
        let namespace = cfg.read_string(f)?;
        let tablespace = cfg.read_string(f)?;
        let table_access_method = cfg.read_string(f)?;
        let owner = cfg.read_string(f)?;
        if cfg.read_string_bool(f)? {
            // This *must* be false
            return Err(ArchiveError::InvalidData(
                "mysterious value must be false".into(),
            ));
        }
        let mut dependencies = Vec::new();
        loop {
            let dep_id = cfg.read_string(f)?;
            if dep_id.is_empty() {
                break;
            }
            dependencies.push(ID::from_str_radix(dep_id.as_str(), 10).or(Err(
                ArchiveError::InvalidData("invalid dependency id".into()),
            ))?);
        }
        let offset = cfg.read_offset(f)?;

        Ok(TocEntry {
            id,
            had_dumper,
            table_oid,
            oid,
            tag,
            desc,
            section,
            defn,
            drop_stmt,
            copy_stmt,
            namespace,
            tablespace,
            table_access_method,
            owner,
            dependencies,
            offset,
        })
    }
}

pub fn read_toc(
    f: &mut (impl Read + ?Sized),
    cfg: &ReadConfig,
) -> Result<Vec<TocEntry>, ArchiveError> {
    let num_entries = cfg.read_int(f)?;
    let mut entries = Vec::with_capacity(num_entries as usize);

    for _ in 0..num_entries {
        entries.push(TocEntry::parse(f, cfg)?);
    }
    Ok(entries)
}

#[cfg(test)]
mod tests {
    use super::*;
    use hex_literal::hex;

    #[test]
    fn encoding_toc_entry() -> Result<(), ArchiveError> {
        let mut input = &hex!(
            "00 8e 11 00 00" // ID
            "00 00 00 00 00" // had dumper
            "00 01 00 00 00 30" // Table OID
            "00 01 00 00 00 30" // OID
            "00 08 00 00 00 45 4e 43 4f 44 49 4e 47" // Tag
            "00 08 00 00 00 45 4e 43 4f 44 49 4e 47" // Desc
            "00 02 00 00 00" // Section
            "00 1e 00 00 00 53 45 54 20 63 6c 69 65 6e 74 5f 65 6e 63 6f 64 69 6e 67 20 3d 20 27 55 54 46 38 27 3b 0a" // Defn
            "01 01 00 00 00" // DropStmt
            "01 01 00 00 00" // CopyStmt
            "01 01 00 00 00" // Namespace
            "01 01 00 00 00" // Tablespace
            "01 01 00 00 00" // TableAccessMethod
            "01 01 00 00 00" // Owner
            "00 05 00 00 00 66 61 6c 73 65" // mandatory false
            "01 01 00 00 00" // end of dependencies
            "03" // offset flag
            "00 00 00 00 00 00 00 00" // offset
        )[..];

        let cfg = ReadConfig {
            int_size: 4,
            offset_size: 8,
        };

        let entry = TocEntry::parse(&mut input, &cfg)?;
        assert_eq!(
            entry,
            TocEntry {
                id: 0x118e,
                had_dumper: false,
                table_oid: 0,
                oid: 0,
                tag: String::from("ENCODING"),
                desc: String::from("ENCODING"),
                section: Section::PreData,
                defn: String::from("SET client_encoding = 'UTF8';\x0a"),
                drop_stmt: String::from(""),
                copy_stmt: String::from(""),
                namespace: String::from(""),
                tablespace: String::from(""),
                table_access_method: String::from(""),
                owner: String::from(""),
                dependencies: vec![],
                offset: Offset::NoData,
            }
        );
        Ok(())
    }

    #[test]
    fn extension_toc_entry() -> Result<(), ArchiveError> {
        let mut input = &hex!(
                "00 02 00 00 00" // ID
                "00 00 00 00 00" // had dumer
                "00 04 00 00 00 33 30 37 39" // Table OID
                "00 05 00 00 00 33 33 37 30 38" // OID
                "00 07 00 00 00 70 6f 73 74 67 69 73" // Tag
                "00 09 00 00 00 45 58 54 45 4e 53 49 4f 4e" // Desc
                "00 02 00 00 00" // Section
                "00 3b 00 00 00 43 52 45 41 54 45 20 45 58 54 45 4e 53 49 4f 4e 20 49 46 20 4e 4f 54 20 45 58 49 53 54 53 20 70 6f 73 74 67 69 73 20 57 49 54 48 20 53 43 48 45 4d 41 20 70 75 62 6c 69 63 3b 0a" // Defn
                "00 18 00 00 00 44 52 4f 50 20 45 58 54 45 4e 53 49 4f 4e 20 70 6f 73 74 67 69 73 3b 0a" // DropStmt
                "01 01 00 00 00" // CopyStmt
                "01 01 00 00 00" // Namespace
                "01 01 00 00 00" // Tablespace
                "01 01 00 00 00" // TableAccessMethod
                "01 01 00 00 00" // Owner
                "00 05 00 00 00 66 61 6c 73 65" // mandatory false
                "01 01 00 00 00" // end of dependencies
                "03" // offset flag
                "00 00 00 00 00 00 00 00" // offset
        )[..];

        let cfg = ReadConfig {
            int_size: 4,
            offset_size: 8,
        };

        let entry = TocEntry::parse(&mut input, &cfg)?;
        assert_eq!(
            entry,
            TocEntry {
                id: 2,
                had_dumper: false,
                table_oid: 3079,
                oid: 33708,
                tag: String::from("postgis"),
                desc: String::from("EXTENSION"),
                section: Section::PreData,
                defn: String::from(
                    "CREATE EXTENSION IF NOT EXISTS postgis WITH SCHEMA public;\x0a"
                ),
                drop_stmt: String::from("DROP EXTENSION postgis;\x0a"),
                copy_stmt: String::from(""),
                namespace: String::from(""),
                tablespace: String::from(""),
                table_access_method: String::from(""),
                owner: String::from(""),
                dependencies: vec![],
                offset: Offset::NoData,
            }
        );
        Ok(())
    }

    #[test]
    fn table_data_toc_entry() -> Result<(), ArchiveError> {
        let mut input = &hex!(
                    "00 8a 11 00 00" // ID
                    "00 01 00 00 00" // HadDumper
                    "00 01 00 00 00 31" // Table OID
                    "00 05 00 00 00 33 33 36 38 36" // OID
                    "00 05 00 00 00 70 69 7a 7a 61" // Tag
                    "00 0a 00 00 00 54 41 42 4c 45 20 44 41 54 41" // Desc
                    "00 03 00 00 00" // Section
                    "01 01 00 00 00" // Defn
                    "01 01 00 00 00" // DropStmt
                    "00 2f 00 00 00 43 4f 50 59 20 70 75 62 6c 69 63 2e 70 69 7a 7a 61 20 28 70 69 7a 7a 61 5f 69 64 2c 20 6e 61 6d 65 29 20 46 52 4f 4d 20 73 74 64 69 6e 3b 0a" // CopyStmt
                    "00 06 00 00 00 70 75 62 6c 69 63" // Namespace
                    "01 01 00 00 00" // Tablespace
                    "01 01 00 00 00" // TableAccessMethod
                    "00 07 00 00 00 77 69 63 68 65 72 74" // Owner
                    "00 05 00 00 00 66 61 6c 73 65" // mandatory false
                    "00 03 00 00 00 32 31 33" // Dependency 1
                    "01 01 00 00 00" // end of dependencies
                    "02" // offset flag
                    "d7 16 00 00 00 00 00 00" // offset
        )[..];

        let cfg = ReadConfig {
            int_size: 4,
            offset_size: 8,
        };

        let entry = TocEntry::parse(&mut input, &cfg)?;
        assert_eq!(
            entry,
            TocEntry {
                id: 0x118a,
                had_dumper: true,
                table_oid: 1,
                oid: 33686,
                tag: String::from("pizza"),
                desc: String::from("TABLE DATA"),
                section: Section::Data,
                defn: String::from(""),
                drop_stmt: String::from(""),
                copy_stmt: String::from("COPY public.pizza (pizza_id, name) FROM stdin;\x0a"),
                namespace: String::from("public"),
                tablespace: String::from(""),
                table_access_method: String::from(""),
                owner: String::from("wichert"),
                dependencies: vec![213],
                offset: Offset::PosSet(0x16d7),
            }
        );
        Ok(())
    }

    #[test]
    fn empty_toc() -> Result<(), ArchiveError> {
        let mut input = &hex!("00 00 00 00 00")[..];
        let cfg = ReadConfig {
            int_size: 4,
            offset_size: 8,
        };

        let toc = read_toc(&mut input, &cfg)?;
        assert!(toc.is_empty());
        Ok(())
    }

    #[test]
    fn single_entry_toc() -> Result<(), ArchiveError> {
        let mut input = &hex!(
            // number of entries
            "00 01 00 00 00"
            // Entry 1
            "00 8e 11 00 00" // ID
            "00 00 00 00 00" // had dumper
            "00 01 00 00 00 30" // Table OID
            "00 01 00 00 00 30" // OID
            "00 08 00 00 00 45 4e 43 4f 44 49 4e 47" // Tag
            "00 08 00 00 00 45 4e 43 4f 44 49 4e 47" // Desc
            "00 02 00 00 00" // Section
            "00 1e 00 00 00 53 45 54 20 63 6c 69 65 6e 74 5f 65 6e 63 6f 64 69 6e 67 20 3d 20 27 55 54 46 38 27 3b 0a" // Defn
            "01 01 00 00 00" // DropStmt
            "01 01 00 00 00" // CopyStmt
            "01 01 00 00 00" // Namespace
            "01 01 00 00 00" // Tablespace
            "01 01 00 00 00" // TableAccessMethod
            "01 01 00 00 00" // Owner
            "00 05 00 00 00 66 61 6c 73 65" // mandatory false
            "01 01 00 00 00" // end of dependencies
            "03" // offset flag
            "00 00 00 00 00 00 00 00" // offset
        )[..];
        let cfg = ReadConfig {
            int_size: 4,
            offset_size: 8,
        };

        let toc = read_toc(&mut input, &cfg)?;
        assert_eq!(toc.len(), 1);
        Ok(())
    }
}
