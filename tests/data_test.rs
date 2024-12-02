use std::fs::File;
use std::io::Read;
use std::path::Path;

#[test]
fn test_table_data() -> Result<(), pgarchive::ArchiveError> {
    let cargo_path = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests");
    let mut f = File::open(cargo_path.join("test.pgdump"))?;
    let archive = pgarchive::Archive::parse(&mut f)?;
    let entry = archive
        .find_toc_entry(pgarchive::Section::Data, "TABLE DATA", "pizza")
        .expect("no data for pizza table present");
    let mut data = archive.read_data(&mut f, &entry)?;
    let mut buffer = Vec::new();
    let size = data.read_to_end(&mut buffer)?;
    assert_eq!(size, 66, "expected 66 bytes, but read {}", size);
    assert_eq!(
        String::from_utf8(buffer).unwrap(),
        "1\tThe Classic\n2\tAll Cheese\n3\tVeggie\n4\tThe Everything\n5\tVegan\n\\.\n\n\n"
    );
    Ok(())
}

#[cfg(feature = "tabledata")]
#[test]
fn test_table_rows() -> Result<(), Box<dyn std::error::Error>> {
    use csv::StringRecord;

    let cargo_path = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests");
    let mut f = File::open(cargo_path.join("test.pgdump"))?;
    let archive = pgarchive::Archive::parse(&mut f)?;
    let mut reader = archive.read_table_rows(&mut f, "pizza")?;
    let rows: Vec<StringRecord> = reader
        .records()
        .into_iter()
        .filter(|r| r.is_ok())
        .map(|r| r.unwrap())
        .collect();
    assert_eq!(rows.len(), 5);
    assert_eq!(
        rows.first().unwrap().iter().collect::<Vec<&str>>(),
        vec!["1", "The Classic"]
    );
    Ok(())
}
