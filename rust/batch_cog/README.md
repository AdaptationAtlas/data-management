A simple set of cli and rust functions to convert raster data (geotiff, asc, etc.) to cloud-optimized geotiffs (COGs), geospatial vectors (gpkg, json, shp, etc.) to geoparquet, and tabular data (csv, tsv, and excel) to parquet in an efficient way. 

It is a work in progress and currently only supports converting raster data to COGs.
The CLI tool in its current state will process a directory of raster data in parallel and accepts a directory as input. 
