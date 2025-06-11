use anyhow::{Error, Result, anyhow};
use gdal::Dataset;
use gdal::Metadata;
use gdal::raster::{Buffer, GdalDataType, RasterBand};
use num_traits::{Float, FromPrimitive, ToPrimitive};
use polars::prelude::*;
use rand::rng;
use rand::seq::SliceRandom;
use rayon::prelude::*;
use std::fs::File;
use std::ops::AddAssign;
use std::path::Path;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use walkdir::WalkDir;

#[derive(Debug)]
pub struct RasterStats {
    pub name: String,
    pub dtype: String,
    pub mean: f64,
    pub min: f64,
    pub max: f64,
    pub variance: f64,
    pub stdev: f64,
    pub cv: f64,
    pub valid_count: u64,
    pub nodata_count: u64,
    pub nan_count: u64,
    pub percent_valid: f64,
    pub q1: Option<f32>,
    pub median: Option<f32>,
    pub q3: Option<f32>,
}
impl RasterStats {
    /// Pretty print a single RasterStats to stdout
    pub fn print(&self) {
        println!("{}", self.format_pretty());
    }

    /// Format a single RasterStats as a pretty string
    pub fn format_pretty(&self) -> String {
        let mut output = String::new();

        output.push_str(&format!("┌─ Band: {} ({})\n", self.name, self.dtype));
        output.push_str(&format!("├─ Statistics:\n"));
        output.push_str(&format!("│  • Mean:     {:>12.6}\n", self.mean));
        output.push_str(&format!("│  • Min:      {:>12.6}\n", self.min));
        output.push_str(&format!("│  • Max:      {:>12.6}\n", self.max));
        output.push_str(&format!("│  • Std Dev:  {:>12.6}\n", self.stdev));
        output.push_str(&format!("│  • Variance: {:>12.6}\n", self.variance));
        output.push_str(&format!("│  • CV:       {:>12.6}\n", self.cv));

        // Add quantiles if available
        if let (Some(q1), Some(median), Some(q3)) = (self.q1, self.median, self.q3) {
            output.push_str(&format!("├─ Quantiles:\n"));
            output.push_str(&format!("│  • Q1:       {:>12.6}\n", q1));
            output.push_str(&format!("│  • Median:   {:>12.6}\n", median));
            output.push_str(&format!("│  • Q3:       {:>12.6}\n", q3));
        }

        output.push_str(&format!("└─ Data Info:\n"));
        output.push_str(&format!(
            "   • Valid:    {:>12} ({:>6.1}%)\n",
            self.valid_count, self.percent_valid
        ));
        output.push_str(&format!("   • NoData:   {:>12}\n", self.nodata_count));
        output.push_str(&format!("   • NaN:      {:>12}\n", self.nan_count));

        output
    }
}

/// Pretty print multiple RasterStats
pub fn print_all_bands(stats: &[RasterStats]) {
    if stats.is_empty() {
        println!("No band statistics to display.");
        return;
    }

    println!("Raster Statistics");

    for (i, stat) in stats.iter().enumerate() {
        println!("{}", stat.format_pretty());
        if i < stats.len() - 1 {
            println!(); // Add a blank line between bands
        }
    }
}

fn percentile<T: Float + ToPrimitive>(sorted: &[T], p: f32) -> f32 {
    if sorted.is_empty() {
        return f32::NAN;
    }
    let idx = ((sorted.len() - 1) as f32 * p).round() as usize;
    sorted.get(idx).and_then(|v| v.to_f32()).unwrap_or(f32::NAN)
}

pub fn compute_stats_generic<T: Float>(band: &RasterBand, quantiles: bool) -> Result<RasterStats>
where
    T: Float + gdal::raster::GdalType + FromPrimitive + ToPrimitive + std::fmt::Debug + AddAssign,
{
    let band_type = band.band_type();
    let (cols, rows) = (band.x_size(), band.y_size());
    let (block_x, block_y) = band.block_size();
    let nodata = band.no_data_value();
    let name = band.description()?;

    // Accumulators
    let mut valid_count = 0u64;
    let mut nodata_count = 0u64;
    let mut nan_count = 0u64;
    let mut sum = T::zero();
    let mut sum_sq = T::zero();
    let mut q1 = None;
    let mut median = None;
    let mut q3 = None;
    let mut min = T::max_value();
    let mut max = T::min_value();

    let nodata_val = nodata.and_then(T::from_f64);
    let epsilon = T::from_f64(1e-6).unwrap();

    let mut process_buffer = |data: &[T]| {
        for &val in data {
            if !val.is_finite() {
                nan_count += 1;
                continue;
            }
            if let Some(nodata_val) = nodata_val {
                if (val - nodata_val).abs() < epsilon {
                    nodata_count += 1;
                    continue;
                }
            }
            valid_count += 1;
            sum += val;
            sum_sq += val * val;
            min = min.min(val);
            max = max.max(val);
        }
    };

    // Hybrid reading
    if quantiles {
        // Full read as required to calcualte quartiles
        let buf: Buffer<T> = band.read_band_as()?;
        let mut valid_values: Vec<T> = Vec::with_capacity(buf.data().len());

        // Single pass to filter valid values and calculate sums
        for &val in buf.data() {
            if !val.is_finite() {
                nan_count += 1;
                continue;
            }
            if let Some(nodata_val) = nodata.and_then(T::from_f64) {
                if (val - nodata_val).abs() < T::from_f64(1e-6).unwrap() {
                    nodata_count += 1;
                    continue;
                }
            }
            valid_values.push(val);
            sum += val;
            sum_sq += val * val;
            min = min.min(val);
            max = max.max(val);
        }
        valid_count = valid_values.len() as u64;

        // Calculate quartiles if we have valid data
        if !valid_values.is_empty() {
            // Sort the data in-place. `partial_cmp` is necessary for floats (f32/f64).
            valid_values
                .sort_unstable_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

            // Use our helper to calculate percentiles
            q1 = Some(percentile(&valid_values, 0.25));
            median = Some(percentile(&valid_values, 0.50));
            q3 = Some(percentile(&valid_values, 0.75));
        }
    } else if block_y == 1 {
        // Row-wise read for non COG
        for row in 0..rows {
            let buf: Buffer<T> = band.read_as((0, row as isize), (cols, 1), (cols, 1), None)?;
            process_buffer(buf.data());
        }
    } else {
        // Tiled layout: block-wise read for COG
        for y in (0..rows).step_by(block_y) {
            for x in (0..cols).step_by(block_x) {
                let win_width = (block_x).min(cols - x);
                let win_height = (block_y).min(rows - y);
                let buf: Buffer<T> = band.read_as(
                    (x as isize, y as isize),
                    (win_width, win_height),
                    (win_width, win_height),
                    None,
                )?;
                process_buffer(buf.data());
            }
        }
    }

    // Final calculations
    let valid_count_f64 = valid_count as f64;
    let sum_f64 = sum.to_f64().unwrap_or(0.0);
    let sum_sq_f64 = sum_sq.to_f64().unwrap_or(0.0);

    let mean = sum_f64 / valid_count_f64;
    let variance = (sum_sq_f64 / valid_count_f64) - mean.powi(2);
    let variance = if variance < 0.0 { 0.0 } else { variance };
    let stdev = variance.sqrt();
    let cv = if mean != 0.0 { stdev / mean } else { 0.0 };
    let percent_valid = valid_count_f64 / (cols * rows) as f64 * 100.0;
    let min = min.to_f64().unwrap_or(0.0);
    let max = max.to_f64().unwrap_or(0.0);

    Ok(RasterStats {
        name,
        dtype: band_type.name(),
        mean,
        min,
        max,
        variance,
        stdev,
        cv,
        valid_count,
        nodata_count,
        nan_count,
        percent_valid,
        q1,
        median,
        q3,
    })
}

pub fn compute_stats(band: &RasterBand, all_stats: bool) -> Result<RasterStats> {
    match band.band_type() {
        GdalDataType::Float64 => compute_stats_generic::<f64>(band, all_stats),
        _ => compute_stats_generic::<f32>(band, all_stats),
    }
}

pub fn compute_all_bands(path: &Path, all_stats: bool) -> Result<Vec<RasterStats>> {
    // println!("Processing: {}", path.display());
    let dataset = Dataset::open(path)?;
    let band_count = dataset.raster_count();
    let mut stats = Vec::with_capacity(band_count as usize);

    for i in 1..=band_count {
        let band = dataset.rasterband(i)?;
        let results = compute_stats(&band, all_stats)?;
        stats.push(results);
    }

    Ok(stats)
}

pub fn raster_stats_to_df(stats: Vec<RasterStats>, filename: &Path) -> LazyFrame {
    let stat_len = stats.len();
    let mut name = Vec::with_capacity(stat_len);
    let mut dtype = Vec::with_capacity(stat_len);
    let mut mean = Vec::with_capacity(stat_len);
    let mut min = Vec::with_capacity(stat_len);
    let mut max = Vec::with_capacity(stat_len);
    let mut variance = Vec::with_capacity(stat_len);
    let mut stdev = Vec::with_capacity(stat_len);
    let mut cv = Vec::with_capacity(stat_len);
    let mut valid_count = Vec::with_capacity(stat_len);
    let mut nodata_count = Vec::with_capacity(stat_len);
    let mut nan_count = Vec::with_capacity(stat_len);
    let mut percent_valid = Vec::with_capacity(stat_len);
    let mut q1 = Vec::with_capacity(stat_len);
    let mut median = Vec::with_capacity(stat_len);
    let mut q3 = Vec::with_capacity(stat_len);

    for s in stats {
        name.push(s.name.clone());
        dtype.push(s.dtype.clone());
        mean.push(s.mean);
        min.push(s.min);
        max.push(s.max);
        variance.push(s.variance);
        stdev.push(s.stdev);
        cv.push(s.cv);
        valid_count.push(s.valid_count as u64);
        nodata_count.push(s.nodata_count as u64);
        nan_count.push(s.nan_count as u64);
        percent_valid.push(s.percent_valid);
        q1.push(s.q1.unwrap_or(f32::NAN));
        median.push(s.median.unwrap_or(f32::NAN));
        q3.push(s.q3.unwrap_or(f32::NAN));
    }

    let file = vec![filename.file_name().unwrap().to_str().unwrap(); stat_len];

    let result_df = DataFrame::new(vec![
        Column::new("file".into(), file),
        Column::new("name".into(), name),
        Column::new("dtype".into(), dtype),
        Column::new("mean".into(), mean),
        Column::new("min".into(), min),
        Column::new("max".into(), max),
        Column::new("variance".into(), variance),
        Column::new("stdev".into(), stdev),
        Column::new("cv".into(), cv),
        Column::new("valid_count".into(), valid_count),
        Column::new("nodata_count".into(), nodata_count),
        Column::new("nan_count".into(), nan_count),
        Column::new("percent_valid".into(), percent_valid),
        Column::new("q1".into(), q1),
        Column::new("median".into(), median),
        Column::new("q3".into(), q3),
    ])
    .unwrap();
    return result_df.lazy();
}

const SUPPORTED_EXTENSIONS: &[&str] = &["tif", "tiff", "asc", "img", "vrt"];

#[derive(Debug, Clone, Copy)]
pub enum OutputFormat {
    Parquet,
    Csv,
}

impl FromStr for OutputFormat {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_lowercase().as_str() {
            "parquet" => Ok(Self::Parquet),
            "csv" => Ok(Self::Csv),
            other => Err(anyhow!(
                "Unsupported output format '{}'. Use 'parquet' or 'csv'.",
                other
            )),
        }
    }
}

impl ToString for OutputFormat {
    fn to_string(&self) -> String {
        match self {
            OutputFormat::Parquet => "parquet".to_string(),
            OutputFormat::Csv => "csv".to_string(),
        }
    }
}

pub fn batch_qaqc(
    directory: &Path,
    pct_check: f32,
    quantiles: bool,
    output_format: OutputFormat,
) -> Result<()> {
    let pct = pct_check.clamp(0.0, 100.0);
    let mut files: Vec<PathBuf> = WalkDir::new(directory)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_file())
        .map(|e| e.into_path())
        .filter(|path| {
            path.extension()
                .and_then(|ext| ext.to_str())
                .map(|ext| SUPPORTED_EXTENSIONS.contains(&ext.to_ascii_lowercase().as_str()))
                .unwrap_or(false)
        })
        .collect();
    let n_total = files.len();
    if n_total == 0 {
        return Err(anyhow!("No files found"));
    }
    let n_sample = ((pct / 100.0) * n_total as f32).ceil() as usize;
    files.shuffle(&mut rng());
    let sample_files = &files[..n_sample];

    let total = sample_files.len();
    let counter = Arc::new(AtomicUsize::new(1));

    let dfs: Vec<LazyFrame> = sample_files
        .par_iter()
        .filter_map(|path| {
            let current = counter.fetch_add(1, Ordering::SeqCst);
            eprintln!(
                "Processing file {}/{}: {:?}",
                current,
                total,
                path.file_name()
            );
            match compute_all_bands(path, quantiles) {
                Ok(df) => Some(raster_stats_to_df(df, path)),
                Err(_) => None, // skip failed files
            }
        })
        .collect();

    assert!(!dfs.is_empty(), "No input dataframes to concatenate.");
    let mut result = concat(&dfs, UnionArgs::default())
        .unwrap()
        .collect()
        .unwrap();

    let mut file = File::create(directory.join("qaqc.parquet")).unwrap();

    match output_format {
        OutputFormat::Csv => {
            CsvWriter::new(&mut file).finish(&mut result)?;
        }
        OutputFormat::Parquet => {
            ParquetWriter::new(&mut file).finish(&mut result)?;
        }
    }
    println!(
        "Wrote output to: {}",
        directory.join("qaqc.parquet").to_str().unwrap()
    );

    Ok(())
}

pub fn single_qaqc(path: &Path, quantiles: bool) -> Result<()> {
    let stats = compute_all_bands(path, quantiles)?;
    println!("{:#?}", stats);
    print_all_bands(&stats);
    Ok(())
}
