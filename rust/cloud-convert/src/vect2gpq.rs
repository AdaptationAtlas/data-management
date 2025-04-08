use gdal::Dataset;
// use gdal::errors::Result;
use gdal::{DriverManager, vector::*};
use std::path::{Path, PathBuf};

/// Converts a vector file to GeoParquet format - simplified version
///
/// # Arguments
/// * `input_path` - Path to the input vector file (any GDAL-supported format)
/// * `output_path` - Path where the GeoParquet file will be written

pub fn vector_to_geoparquet(input_path: &Path, output_path: Option<&Path>) -> Result<(), String> {
    // Validate input path
    if !input_path.exists() {
        return Err(format!(
            "Input path '{}' does not exist",
            input_path.display()
        ));
    }

    // Determine output path
    let out_path = match output_path {
        Some(p) => p.to_path_buf().with_extension("parquet"),
        None => {
            let mut out = input_path.with_extension("parquet");
            // fallback if input path has no file name
            if out.file_name().is_none() {
                out = PathBuf::from("output.parquet");
            }
            out
        }
    };

    // Open the source dataset
    let dataset_src = Dataset::open(input_path).expect(&format!(
        "Failed to open source dataset: {}",
        input_path.display()
    ));

    // Ensure dataset has layers
    if dataset_src.layer_count() == 0 {
        return Err("Source dataset contains no layers".to_string());
    }

    let mut layer_src = dataset_src
        .layer(0)
        .expect("Failed to access first layer of dataset");

    let spatial_ref_src = layer_src.spatial_ref();

    // Get field definitions from source layer
    let fields_defn = layer_src
        .defn()
        .fields()
        .map(|field| (field.name(), field.field_type(), field.width()))
        .collect::<Vec<_>>();

    // Create output dataset with Parquet driver
    let drv = DriverManager::get_driver_by_name("Parquet").expect("Failed to get Parquet driver");

    let out_path_str = out_path
        .to_str()
        .expect("Output path contains invalid UTF-8 characters");

    let mut ds_dest = drv.create_vector_only(out_path_str).expect(&format!(
        "Failed to create destination dataset at {}",
        out_path.display()
    ));

    // Create layer in the destination dataset
    let lyr_dest = ds_dest
        .create_layer(LayerOptions {
            srs: spatial_ref_src.as_ref(),
            ..Default::default()
        })
        .expect("Failed to create destination layer");

    // Copy field schema from source to destination
    for fd in &fields_defn {
        let field_defn = FieldDefn::new(&fd.0, fd.1)
            .expect(&format!("Failed to create field definition for '{}'", fd.0));

        field_defn.set_width(fd.2);
        field_defn
            .add_to_layer(&lyr_dest)
            .expect(&format!("Failed to add field '{}' to layer", fd.0));
    }

    // Get layer definition for creating features
    let defn = Defn::from_layer(&lyr_dest);

    // Copy all features from source to destination
    for feature_src in layer_src.features() {
        // Create new feature
        let mut feature_dest = Feature::new(&defn).expect("Failed to create feature");

        // Copy geometry directly without transformation
        if let Some(geom) = feature_src.geometry() {
            feature_dest
                .set_geometry(geom.clone())
                .expect("Failed to set geometry");
        }

        // Copy field values
        for idx in 0..fields_defn.len() {
            if let Some(value) = feature_src
                .field(idx)
                .expect(&format!("Failed to read field {}", idx))
            {
                feature_dest
                    .set_field(idx, &value)
                    .expect(&format!("Failed to set field {}", idx));
            }
        }

        // Add feature to destination layer
        feature_dest
            .create(&lyr_dest)
            .expect("Failed to create feature in destination");
    }

    println!(
        "Successfully converted {} to GeoParquet: {}",
        input_path.display(),
        out_path.display()
    );

    Ok(())
}
