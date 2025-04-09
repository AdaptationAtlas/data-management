use cloud_convert::batch_convert;
use std::path::Path;

#[test]
fn test_batch_convert_cog() {
    let input = Path::new("tests/data/batch_data");
    let out_dir = Some(Path::new("tests/data/batch_data/out"));
    let result = batch_convert::batch_convert_cog(&input, out_dir, true);
    assert!(result.is_ok());
}

#[test]
fn test_batch_convert_gpq() {
    let input = Path::new("tests/data/batch_data");
    let out_dir = Some(Path::new("tests/data/batch_data/out"));
    let result = batch_convert::batch_convert_gpq(&input, out_dir);
    assert!(result.is_ok(), "Batch convert failed: {:?}", result.err());
    let summary = result.unwrap();
    assert_eq!(summary.successful.len(), 4); // 3 files converted
    // assert_eq!(summary.failed.len(), 1);     // 1 file failed with the max ojs size off

    // assert_eq!(
    //     summary.failed[0].0.file_name().unwrap(),
    // "bigfile.geojson"
    // );
}



