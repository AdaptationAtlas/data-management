use clap::{Parser, Subcommand};
use std::path::PathBuf;

mod batch_convert;
mod datainfo;
mod rast_qaqc;
mod tif2cog;
mod vect2gpq;

use batch_convert::*;
use datainfo::*;
use rast_qaqc::*;
use tif2cog::*;
use vect2gpq::*;

#[derive(Parser)]
#[command(name = "cloud_convert")]
#[command(about = "Geospatial file utilities", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Show information about a geospatial file
    Info { path: PathBuf },

    /// Convert raster to Cloud-Optimized GeoTIFF
    ToCog {
        path: PathBuf,
        #[arg(short, long)]
        out: Option<PathBuf>,
        #[arg(short, long, default_value_t = false)]
        overwrite: bool,
    },

    /// Convert vector to GeoParquet
    ToGpq {
        path: PathBuf,
        #[arg(short, long)]
        out: Option<PathBuf>,
    },

    /// Get stats for QAQC for a GeoTIFF
    RunQAQC {
        path: PathBuf,
        #[arg(short, long, default_value_t = false)]
        quantiles: bool,
        #[arg(short, long, default_value_t = 100)]
        pct_check: u8,
        #[arg(short, long, default_value_t = OutputFormat::Csv)]
        output_format: OutputFormat,
    },
}

fn main() {
    let cli = Cli::parse();

    match cli.command {
        Commands::Info { path } => match get_datainfo(&path) {
            Ok(info) => print_datainfo(&info),
            Err(e) => eprintln!("Error: {}", e),
        },

        Commands::ToCog {
            path,
            out,
            overwrite,
        } => {
            if path.is_dir() {
                if let Err(e) = batch_convert_cog(&path, out.as_deref(), overwrite) {
                    eprintln!("Batch COG conversion failed: {}", e);
                }
            } else {
                if let Err(e) = tif_to_cog(&path, out.as_deref(), overwrite) {
                    eprintln!("Single COG conversion failed: {}", e);
                }
            }
        }

        Commands::ToGpq { path, out } => {
            if path.is_dir() {
                if let Err(e) = batch_convert_gpq(&path, out.as_deref()) {
                    eprintln!("Batch GPQ conversion failed: {}", e);
                }
            } else {
                if let Err(e) = vector_to_geoparquet(&path, out.as_deref()) {
                    eprintln!("Single GPQ conversion failed: {}", e);
                }
            }
        }
        Commands::RunQAQC {
            path,
            pct_check,
            output_format,
            quantiles,
        } => {
            if path.is_dir() {
                if let Err(e) = batch_qaqc(&path, pct_check as f32, quantiles, output_format) {
                    eprintln!("Batch QAQC failed: {}", e);
                }
            } else {
                if let Err(e) = single_qaqc(&path, quantiles) {
                    eprintln!("Single QAQC failed: {}", e);
                }
            }
        }
    }
}
