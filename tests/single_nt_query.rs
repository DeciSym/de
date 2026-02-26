use de::*;
use de::rdf2nt::Rdf2Nt;
use std::path::Path;
use std::sync::Arc;
use tempfile::Builder;

#[test]
fn test_files_to_rdf_single_nt_returns_temporary_output_path() -> anyhow::Result<()> {
    let source_ttl = "tests/resources/rdf-tests/sparql/sparql11/bind/data.ttl";
    assert!(Path::new(source_ttl).exists(), "missing W3C fixture: {source_ttl}");

    let nt_input = Builder::new().suffix(".nt").append(true).tempfile()?;
    let convert_res =
        rdf2nt::OxRdfConvert {}.convert_to_nt(vec![source_ttl.to_string()], nt_input.as_file())?;
    assert_eq!(convert_res.converted, 1);

    let source_nt = nt_input.path().to_string_lossy().to_string();
    let mut out_file = Builder::new().suffix(".nt").append(true).tempfile()?;
    let data_files = vec![source_nt.clone()];

    let (combined_path, unknown_files) =
        create::files_to_rdf(&data_files, &mut out_file, Arc::new(rdf2nt::OxRdfConvert {}))?;

    assert!(unknown_files.is_empty());
    assert_ne!(
        combined_path,
        source_nt,
        "single .nt input should be normalized to conversion output path"
    );

    Ok(())
}
