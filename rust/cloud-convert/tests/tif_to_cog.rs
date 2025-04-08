use cloud_convert::tif2cog::tif_to_cog;
use std::path::Path;

#[test]
fn test_tif_to_cog() {
    let input = Path::new("tests/data/test_input.tif");
    let output_path: Option<&Path> = None;
    tif_to_cog(input, output_path, true).unwrap();
}
