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
    assert!(result.is_ok());
}
