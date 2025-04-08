use cloud_convert::datainfo::{get_datainfo, print_datainfo};
use std::path::Path;

#[test]
fn test_datainfo_get() {
    let datainfo = get_datainfo(&Path::new("tests/data/test_input.gpkg")).unwrap();

    let lyrs = datainfo.layers.unwrap();
    let lyr1 = lyrs.get(0).unwrap();
    // let lyr1_name = lyr1.name.clone();
    let lyr1_crs = lyr1.crs.clone().unwrap();
    assert_eq!(
        lyr1_crs, "WGS 84",
        "The CRS of the first layer is incorrect. Expected 'WGS 84' but found '{}'.",
        lyr1_crs
    );
    assert_eq!(
        lyr1.name.clone(),
        "atlas_gaul_a0_africa_verysimple",
        "Layer names don't match"
    );
}

#[test]
fn test_datainfo_print() {
    let datainfo = get_datainfo(&Path::new("tests/data/test_input.gpkg")).unwrap();
    print_datainfo(&datainfo);
}

#[test]
fn test_datainfo_tif() {
    let datainfo = get_datainfo(&Path::new("tests/data/test_input.tif")).unwrap();
    print_datainfo(&datainfo);
    let rast_size = datainfo.size.unwrap();
    assert_eq!(rast_size, (828, 746), "Raster size is incorrect");
}
