use std::fs::File;
use std::io::Read;
use std::path::Path;

#[test]
fn test_table_data() -> Result<(), pgarchive::ArchiveError> {
    let cargo_path = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests");
    let mut f = File::open(cargo_path.join("test.pgdump"))?;
    let archive = pgarchive::Archive::parse(&mut f)?;
    let entry = archive
        .get_toc_entry(pgarchive::Section::Data, "pizza")
        .expect("no data for pizza table present");
    let mut data = archive.read_data(&mut f, entry.offset)?;
    let mut buffer = Vec::new();
    let size = data.read_to_end(&mut buffer)?;
    assert_eq!(size, 69, "expected 69 bytes, but read {}", size);
    Ok(())
}
