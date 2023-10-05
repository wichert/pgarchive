use crate::archive::Archive;
use crate::types::{ArchiveError, Section};
use pg_query::{NodeEnum, NodeRef};
use std::fs::File;
use std::io::Read;

#[cfg(feature = "tabledata")]
pub fn table_data_reader(
    archive: &Archive,
    file: &mut File,
    table: &str,
) -> Result<csv::Reader<Box<dyn Read>>, ArchiveError> {
    let create_entry = archive
        .find_toc_entry(Section::PreData, "TABLE", table)
        .ok_or(ArchiveError::NoDataPresent)?;
    let columns = table_column_names(&create_entry.defn).or(Err(ArchiveError::InvalidData(
        "invalid CREATE TABLE statement".into(),
    )))?;

    let data_entry = archive
        .find_toc_entry(Section::Data, "TABLE DATA", table)
        .ok_or(ArchiveError::NoDataPresent)
        .unwrap();
    let data = archive.read_data(file, data_entry).unwrap();
    let mut rdr = csv::ReaderBuilder::new()
        .delimiter(b'\t')
        .quoting(false)
        .flexible(false)
        .from_reader(data);
    rdr.set_headers(columns.into());
    Ok(rdr)
}

#[cfg(feature = "tabledata")]
fn table_column_names(create_stmt: &str) -> Result<Vec<String>, pg_query::Error> {
    let result = pg_query::parse(create_stmt)?;
    let stmt = result.protobuf.nodes()[0].0;
    match stmt {
        NodeRef::CreateStmt(table_info) => Ok(table_info
            .table_elts
            .iter()
            .filter_map(|e| match &e.node {
                Some(NodeEnum::ColumnDef(cd)) => Some(cd.as_ref().colname.clone()),
                _ => None,
            })
            .collect()),
        _ => Err(pg_query::Error::Parse("invalid statement type".into())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_table_column_names() {
        assert!(table_column_names(
            "CREATE DATABASE pizza WITH TEMPLATE = template0 ENCODING = 'UTF8' LOCALE = 'C';"
        )
        .is_err());

        let columns = table_column_names(
            "CREATE TABLE public.pizza (pizza_id integer NOT NULL, name text NOT NULL);",
        );
        assert!(columns.is_ok());
        let columns = columns.unwrap();
        assert_eq!(columns, vec!["pizza_id", "name"]);
    }
}
