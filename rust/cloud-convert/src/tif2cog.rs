use gdal::Dataset;
use gdal::DriverManager;
use gdal::raster::RasterCreationOptions;
use std::path::Path;

pub fn tif_to_cog(
    input_path: &Path,
    output_path: Option<&Path>,
    overwrite: bool,
) -> Result<(), String> {
    // Check if the input file exists
    if !input_path.exists() {
        return Err(format!("Error: The file {:?} does not exist.", input_path));
    }

    let out_path = match output_path {
        Some(path) => {
            // Warn if the output file exists and will be overwritten
            if path.exists() && !overwrite {
                return Err(format!(
                    "Error: The file {:?} already exists and overwrite is false.",
                    path
                ));
            }
            path.to_path_buf().with_extension("tif")
        }
        None => {
            let mut out_path = input_path.to_path_buf();

            if overwrite {
                // If overwrite is true, use the input name as output name
                out_path
            } else {
                // If overwrite is false, append "_cog" to the input name
                let extension = out_path
                    .extension()
                    .unwrap_or_default()
                    .to_str()
                    .unwrap_or("");
                out_path.set_file_name(format!(
                    "{}_cog.{}",
                    out_path
                        .file_stem()
                        .unwrap_or_default()
                        .to_str()
                        .unwrap_or(""),
                    extension
                ));

                // If the output file exists, return an error
                if out_path.exists() {
                    return Err(format!(
                        "Error: The file {:?} already exists and overwrite is false.",
                        out_path
                    ));
                }

                out_path.with_extension("tif")
            }
        }
    };
    println!("Output will be saved to: {:?}", out_path);

    // Open the dataset and handle errors
    let dataset = Dataset::open(input_path.to_str().unwrap())
        .map_err(|e| format!("Failed to open dataset: {:?}", e))?;

    // Get the driver
    let driver = DriverManager::get_driver_by_name("COG")
        .expect("Failed to get COG driver, is GDAL up to date?");

    let creation_options = RasterCreationOptions::from_iter(["COMPRESS=LZW"]);

    // Attempt to create the copy, handling any errors
    dataset
        .create_copy(&driver, out_path.to_str().unwrap(), &creation_options)
        .map_err(|e| format!("Failed to create COG: {:?}", e))?;

    Ok(())
}
