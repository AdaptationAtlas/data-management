use gdal::Dataset;
use gdal::vector::OGRFieldType;
use std::path::Path;
// use gdal::spatial_ref::SpatialRef;
use gdal::vector::LayerAccess;

fn field_type_to_str(ftype: u32) -> &'static str {
    match ftype {
        OGRFieldType::OFTInteger => "Integer",
        OGRFieldType::OFTIntegerList => "IntegerList",
        OGRFieldType::OFTReal => "Real",
        OGRFieldType::OFTRealList => "RealList",
        OGRFieldType::OFTString => "String",
        OGRFieldType::OFTStringList => "StringList",
        OGRFieldType::OFTBinary => "Binary",
        OGRFieldType::OFTDate => "Date",
        OGRFieldType::OFTTime => "Time",
        OGRFieldType::OFTDateTime => "DateTime",
        _ => "Unknown",
    }
}

#[derive(Debug)]
pub enum DatasetType {
    Raster,
    Vector,
}

#[derive(Debug)]
pub struct LayerInfo {
    pub name: String,
    pub crs: Option<String>,
    pub fields: Vec<(String, String)>,
    pub feature_count: u64,
}

#[derive(Debug)]
pub struct DatasetInfo {
    pub dataset_type: DatasetType,
    pub driver: String,
    pub crs: Option<String>,
    pub size: Option<(usize, usize)>,
    pub band_count: Option<usize>,
    pub layers: Option<Vec<LayerInfo>>,
    pub layer_count: Option<usize>,
}

pub fn get_datainfo(path: &Path) -> gdal::errors::Result<DatasetInfo> {
    let ds = Dataset::open(path)?;
    let driver = ds.driver().short_name().to_string();
    let band_count = ds.raster_count();
    let layer_count = ds.layer_count();

    if band_count > 0 {
        // Raster dataset
        let crs = ds.spatial_ref().ok().and_then(|r| r.name());

        Ok(DatasetInfo {
            dataset_type: DatasetType::Raster,
            driver,
            crs,
            size: Some(ds.raster_size()),
            band_count: Some(band_count),
            layer_count: None,
            layers: None,
        })
    } else {
        // Vector dataset
        let mut layers_info = vec![];

        for idx in 0..layer_count {
            let layer = ds.layer(idx)?;
            let crs = layer.spatial_ref().and_then(|r| r.name());
            // .unwrap_or("Unknown CRS".to_string());
            let name = layer.name();
            let feature_count = layer.feature_count();

            let fields = layer
                .defn()
                .fields()
                .map(|f| (f.name(), field_type_to_str(f.field_type()).to_string()))
                .collect::<Vec<_>>();

            layers_info.push(LayerInfo {
                name,
                crs,
                fields,
                feature_count,
            });
        }

        Ok(DatasetInfo {
            dataset_type: DatasetType::Vector,
            driver,
            crs: None,
            size: None,
            band_count: None,
            layer_count: Some(layer_count),
            layers: Some(layers_info),
        })
    }
}

pub fn print_datainfo(info: &DatasetInfo) {
    match info.dataset_type {
        DatasetType::Raster => {
            println!("Raster dataset:");
            println!("Driver: {}", info.driver);
            println!(
                "Size: {:?} x {:?} pixels",
                info.size.unwrap().0,
                info.size.unwrap().1
            );
            println!("Band count: {}", info.band_count.unwrap());
            println!("CRS: {}", info.crs.clone().unwrap_or("Unknown".to_string()));
        }
        DatasetType::Vector => {
            println!("Vector dataset:");
            println!("Driver: {}", info.driver);
            println!("Layer count: {}", info.layer_count.unwrap());
            for layer in info.layers.as_ref().unwrap() {
                println!("Layer: {}", layer.name);
                println!("Feature count: {}", layer.feature_count);
                println!("Fields:");
                for (name, ftype) in &layer.fields {
                    println!("  {}: {}", name, ftype);
                }
                println!(
                    "CRS: {}",
                    layer.crs.clone().unwrap_or("Unknown".to_string())
                );
            }
        }
    }
}
