use cloud_convert::vect2gpq::vector_to_geoparquet;

#[test]
fn test_vector_to_geoparquet() {
    let input_path = std::path::Path::new("tests/data/test_input.gpkg");
    let output_path = std::path::Path::new("tests/data/test_output.parquet");

    vector_to_geoparquet(input_path, Some(output_path)).unwrap();
}
