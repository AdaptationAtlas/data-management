extern crate gdal;

use gdal::Dataset;
use gdal::DriverManager;
use gdal::raster::RasterCreationOption;
use std::path::Path;
use std::path::PathBuf;
use std::fs;
use std::env;
use rayon::prelude::*;





fn create_cog(filepath: &Path) {
    println!("Creating COG for {}", filepath.display());

    let basename = filepath.file_stem().unwrap().to_str();
    let dir = filepath.parent().unwrap();
    

    println!("Basename: {}", basename.unwrap());

    // let tif_path = format!("{}/{}.tif", folder, basename.unwrap());
    let tif_path = dir.join(format!("{}.tif", basename.unwrap()));

    let dataset = Dataset::open(filepath)
        .expect("Failed to open dataset");

    let driver = DriverManager::get_driver_by_name("COG")
        .expect("Failed to get driver, is GDAL up to date?");

    let creation_options = [
        RasterCreationOption {
            key: "COMPRESS",
            value: "LZW"
        }
    ];

    let _cog = dataset
        .create_copy(
            &driver,
            tif_path.to_str().unwrap(),
            &creation_options
        );
}

fn main() {
    let start_time = std::time::Instant::now();

    let args: Vec<String> = env::args().collect();

    if args.len() != 2 {
        eprintln!("Usage: {} <path>", args[0]);
        std::process::exit(1);
    }

    let path = Path::new(&args[1]);

    if path.is_dir() {
        let files: Vec<PathBuf> = fs::read_dir(path)
        .expect("Failed to read files from directory")
        .map(|entry| entry.unwrap().path())
        .collect();

        files.par_iter().for_each(|x| create_cog(x));
    } else if path.is_file() {
        create_cog(path);
    } else {
        eprintln!("Invalid path: {}", path.display());
        std::process::exit(1);
    }

    let total_time = start_time.elapsed();
    println!("Total processing time: {:?}", total_time);
}