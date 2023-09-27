use std::fs::File;
use std::path::Path;

#[test]
fn test_parse_header() -> Result<(), pgarchive::ArchiveError> {
    let cargo_path = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests");
    let mut f = File::open(cargo_path.join("test.pgdump"))?;
    let archive = pgarchive::Archive::parse(&mut f)?;
    assert_eq!(archive.database_name, "pizza");
    assert_eq!(
        archive.compression_method,
        pgarchive::CompressionMethod::Gzip
    );
    assert_eq!(archive.compression_level, -1);
    assert_eq!(
        archive
            .toc_entries
            .iter()
            .filter(|e| e.section == pgarchive::Section::Data && e.desc == "TABLE DATA")
            .map(|e| e.tag.clone())
            .collect::<Vec<String>>(),
        vec!["pizza", "pizza_topping", "topping"]
    );

    Ok(())
}
