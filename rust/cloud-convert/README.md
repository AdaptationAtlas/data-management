# cloud_convert

`cloud_convert` is a Rust-based command-line tool for processing geospatial files. It supports inspecting file metadata, converting raster files to Cloud-Optimized GeoTIFFs (COGs), and converting vector files to GeoParquet. Batch processing is automatically handled when directories are provided as input.

---

## Why?? Performance
Sometimes large folders of raster and vector files need converted. Although this can be done through an R script, this package is much faster and is designed to handle errors better. 

In prelim testing, converting a folder of 340 geojson files ranging in size from 600mb to < 1mb:
- R terra with multicore: 4 minutes 3 seconds
- This package: 1 minute 15 seconds


## Features

- Inspect metadata of raster and vector files
- Convert raster files to Cloud-Optimized GeoTIFF (COG)
- Convert vector files to GeoParquet
- Automatically detects and processes directories in batch

---

## Installation

Install locally using Cargo:

```bash
cargo install --path .
# Or build it locally and run the resulting binary from ./target/release/cloud_convert
cargo build --release
```


Got it — here’s a more professional version of the `README.md`, still friendly but toned down:

---

```markdown
# cloud_convert

`cloud_convert` is a Rust-based command-line tool for processing geospatial files. It supports inspecting file metadata, converting raster files to Cloud-Optimized GeoTIFFs (COGs), and converting vector files to GeoParquet. Batch processing is automatically handled when directories are provided as input.

---

## Features

- Inspect metadata of raster and vector files
- Convert raster files to Cloud-Optimized GeoTIFF (COG)
- Convert vector files to GeoParquet
- Automatically detects and processes directories in batch

---

## Installation

Install locally using Cargo:

```bash
cargo install --path .
```

Or build manually:

```bash
cargo build --release
```

This will create the binary at `./target/release/cloud_convert`.

---

## Usage

### Inspect file metadata

```bash
cloud_convert info path/to/file.tif
cloud_convert info path/to/file.gpkg
```

---

### Convert raster to COG

Convert a single raster file:

```bash
cloud_convert to-cog path/to/file.tif --overwrite
```

Convert all `.tif` files in a directory:

```bash
cloud_convert to-cog path/to/folder --out path/to/output_dir --overwrite
```

---

### Convert vector to GeoParquet

Convert a single vector file:

```bash
cloud_convert to-gpq path/to/file.gpkg --out output.parquet
```

Convert all vector files in a directory:

```bash
cloud_convert to-gpq path/to/folder --out path/to/output_dir
```

---

## Running Tests

Run all unit tests:

```bash
cargo test
```
---

