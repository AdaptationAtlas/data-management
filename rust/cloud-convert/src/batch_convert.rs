use crate::tif2cog::tif_to_cog;
use crate::vect2gpq::vector_to_geoparquet;
use rayon::prelude::*;
use std::fs;
use std::path::Path;
use std::path::PathBuf;

fn batch_convert<F>(
    input_path: &Path,
    output_dir: Option<&Path>,
    extensions: &[&str],
    file_type: &str,
    converter: F,
) -> Result<(), String>
where
    F: Fn(&Path, Option<&Path>) -> Result<(), String> + Send + Sync,
{
    if !input_path.is_dir() {
        return Err(format!(
            "Input path '{}' is not a directory",
            input_path.display()
        ));
    }

    // Create output directory if specified and doesn't exist
    if let Some(out_dir) = output_dir {
        if !out_dir.exists() {
            fs::create_dir_all(out_dir)
                .map_err(|e| format!("Failed to create output directory: {}", e))?;
        }
    }

    let files: Vec<PathBuf> = input_path
        .read_dir()
        .map_err(|e| format!("Failed to read directory: {}", e))?
        .filter_map(Result::ok)
        .filter(|entry| {
            entry
                .path()
                .extension()
                .and_then(|e| e.to_str())
                .map(|ext| extensions.contains(&ext.to_lowercase().as_str()))
                .unwrap_or(false)
        })
        .map(|entry| entry.path())
        .collect();

    if files.is_empty() {
        return Err(format!(
            "No supported {} files found in '{}'",
            file_type,
            input_path.display()
        ));
    }

    let errors: Vec<(PathBuf, String)> = files
        .par_iter()
        .filter_map(|path| {
            // Prepare output path with file name from input
            let file_output_path = output_dir.map(|out_dir| {
                let file_name = path.file_name().unwrap_or_default();
                out_dir.join(file_name)
            });

            match converter(path, file_output_path.as_deref()) {
                Ok(_) => None,
                Err(e) => Some((path.clone(), e)),
            }
        })
        .collect();

    let total = files.len();
    let failed = errors.len();
    let succeeded = total - failed;

    if failed > 0 {
        let mut error_msg = format!(
            "Converted {}/{} files. Errors occurred:\n",
            succeeded, total
        );
        for (path, err) in errors {
            error_msg.push_str(&format!("- {}: {}\n", path.display(), err));
        }
        Err(error_msg)
    } else {
        Ok(())
    }
}

pub fn batch_convert_cog(
    input_path: &Path,
    output_dir: Option<&Path>,
    overwrite: bool,
) -> Result<(), String> {
    let raster_exts = ["tif", "tiff", "tff", "asc", "img"];
    batch_convert(
        input_path,
        output_dir,
        &raster_exts,
        "raster",
        |path, out_path| tif_to_cog(path, out_path, overwrite),
    )
}

pub fn batch_convert_gpq(input_path: &Path, output_dir: Option<&Path>) -> Result<(), String> {
    let vector_exts = ["gpkg", "json", "geojson", "fgb", "kml", "gpx", "shp"];
    batch_convert(
        input_path,
        output_dir,
        &vector_exts,
        "vector",
        |path, out_path| vector_to_geoparquet(path, out_path),
    )
}

