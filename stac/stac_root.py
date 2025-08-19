import pystac
import s3fs

### Main catalog root ###
atlas_catalog = pystac.Catalog(
    id="adaptation-atlas",
    description="The Agriculture Adaptation Atlas utilizes a SpatioTemporal Asset Catalog (STAC) to support its vision of being an accessiable and collabrative tool for sharing and creating critical data and data stories for decision-makers. The STAC metadata ensures accessibility and usability by following an established and widely used community standard. This allows for greater collaboration, interoerability with geospatial tools and programming languages, and easier searching and quering data. By leveraging STAC and cloud-optimized data formats, we streamline access to our data and allow others to bring their data to us. The data included in this catalog are often larger supersets of what is used in the atlas. If you need a small portion of data to replicate a plot or notebook, it is suggested to download these directly from the observable notebooks. If you want to perform calculations using larger raw and analysis-ready datasets, use the STAC catalog to find the data you are looking for.",
    title="Africa Agriculture Adaptation Atlas Catalog",
)

atlas_site = pystac.Link(
    rel="related",
    target="https://adaptationatlas.cgiar.org/",
    title="Africa Agriculture Adaptation Atlas Website",
    media_type="text/html",
)

atlas_catalog.add_link(atlas_site)

### Theme Catalogs ###

## Boundaries ##
boundaries_catalog = pystac.Catalog(
    id="boundaries",
    title="Boundaries",
    description=(
        "Datasets defining geographic boundaries used in the Adaptation Atlas. "
        "Includes watersheds, administrative areas, and other relevant spatial units."
    ),
)
atlas_catalog.add_child(boundaries_catalog)

## Exposure ##
exposure_catalog = pystac.Catalog(
    id="exposure",
    title="Exposure",
    description=(
        "Datasets representing the presence or distribution of people, assets, or systems "
        "that could be affected by climate-related hazards."
    ),
)
atlas_catalog.add_child(exposure_catalog)

## Socio-Economic ##
socioeconomic_catalog = pystac.Catalog(
    id="socio-economic",
    title="Socio-Economic",
    description=(
        "Socio-economic indicators relevant to climate vulnerability and adaptive capacity. "
        "Includes datasets on poverty, livelihoods, readiness, and related metrics."
    ),
)
atlas_catalog.add_child(socioeconomic_catalog)

## Impacts ##
impacts = pystac.Catalog(
    id="impacts",
    title="Impacts",
    description=(
        "Datasets quantifying the impacts of climate change on key systems. "
        "Includes changes in crop suitability and yields, pest and disease risks, and related indicators."
    ),
)
atlas_catalog.add_child(impacts)

## Solutions ##
solutions_catalog = pystac.Catalog(
    id="solutions",
    title="Solutions",
    description=(
        "Datasets highlighting adaptation solutions to climate risks. "
        "Includes information on solution types, their spatial suitability, and projected outcomes such as yields "
        "under different adaptation strategies. Some datasets may overlap with those in the Impacts catalog."
    ),
)
atlas_catalog.add_child(solutions_catalog)

## Phenology ##
phenology = pystac.Catalog(
    id="phenology",
    title="Phenology",
    description=(
        "Datasets describing the timing and duration of seasonal biological and agricultural events. "
        "Includes start and end of season, season length, and crop calendars, based on both historical observations "
        "and future climate projections."
    ),
)
atlas_catalog.add_child(phenology)

## Hazard Exposure ##
hazard_exposure = pystac.Catalog(
    id="hazard-exposure",
    title="Hazard Exposure",
    description=(
        "Datasets showing historical and projected exposure of crops, livestock and people "
        "to climate variables and hazards, such as droughts, waterlogging, heatstress, "
        "precipitation variability and other events."
    ),
)
atlas_catalog.add_child(hazard_exposure)

## Climate ##
climate = pystac.Catalog(
    id="climate",
    title="Climate",
    description=(
        "Climate datasets used in the Adaptation Atlas. Includes historical observations and future projections "
        "of both climate hazards and underlying variables (e.g., temperature, precipitation)."
    ),
)
atlas_catalog.add_child(climate)

## Finalize and Save ###

atlas_catalog.normalize_and_save(
    catalog_type=pystac.CatalogType.SELF_CONTAINED, root_href="dev-stac"
)

s3 = s3fs.S3FileSystem()

s3.upload("dev-stac/", "digital-atlas/stac/dev_stac/", recursive=True)
