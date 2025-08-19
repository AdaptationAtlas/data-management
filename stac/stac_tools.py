import pyarrow.parquet as pq
import json
import obstore
import pyproj
import pystac
from pystac.extensions import file
import os
from datetime import datetime, timezone
from dataclasses import dataclass


def current_time() -> str:
    """Get current UTC timestamp in RFC 3339 format.
    Returns:
        str: Current UTC timestamp in RFC 3339 format
    """
    return (
        datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    )


def current_time() -> str:
    """Get datetime to timestamp in RFC 3339 format.
    Returns:
        str: Current UTC timestamp
    """
    return (
        datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    )


def get_parquet_mdata(path: str) -> dict:
    """
    Extracts metadata from a Parquet file, including geospatial metadata if a GeoParquet file.

    Args:
        path (str): Path to the Parquet file.

    Returns:
        dict: A dictionary containing:
            bbox (list | None): Bounding box coordinates.
            crs_proj (pyproj.CRS | None): PROJJSON of the coordinate reference system (CRS).
            crs (str | None): CRS in EPSG format.
            columns (list): List of column names.
            dtypes (list): List of column data types.
            num_rows (int): Number of rows in the parquet file.
    """
    mdata = pq.read_metadata(path)
    schema = mdata.schema.to_arrow_schema()
    geo_mdata_raw = mdata.metadata.get(b"geo")

    if geo_mdata_raw:
        geo = json.loads(geo_mdata_raw.decode("utf-8"))
        geom_col = geo.get("primary_column")
        gpq_schema = geo.get("version")
        bbox = geo["columns"]["geometry"].get("bbox")
        crs_raw = geo["columns"]["geometry"].get("crs", None)
        if isinstance(crs_raw, dict):
            crs_proj = pyproj.CRS.from_dict(crs_raw)
        else:
            crs_proj = pyproj.CRS.from_epsg(
                4326
            )  # crs is opt gpq metadata, default is WGS84
        crs = crs_proj.to_epsg()
    else:
        gpq_schema = geom_col = bbox = crs_proj = crs = None

    cols = schema.names
    dtypes = schema.types
    nrow = mdata.num_rows

    return {
        "gpq_schema": f"v{gpq_schema}",
        "geom_col": geom_col,
        "bbox": bbox,
        "crs_proj": crs_proj,
        "crs": crs,
        "columns": cols,
        "dtypes": dtypes,
        "num_rows": nrow,
    }


def get_s3_store(
    bucket: str = "digital-atlas", region: str = "us-east-1"
) -> obstore.store.S3Store:
    # Check if AWS env vars already set
    if not (
        os.environ.get("AWS_ACCESS_KEY_ID") and os.environ.get("AWS_SECRET_ACCESS_KEY")
    ):
        cred_file = os.path.expanduser("~/.aws/credentials")
        if os.path.exists(cred_file):
            with open(cred_file, "r") as f:
                lines = f.readlines()
            for line in lines:
                if "aws_access_key_id" in line.lower():
                    os.environ["AWS_ACCESS_KEY_ID"] = line.split("=", 1)[1].strip()
                elif "aws_secret_access_key" in line.lower():
                    os.environ["AWS_SECRET_ACCESS_KEY"] = line.split("=", 1)[1].strip()

    # Determine whether to skip signature based on env vars presence
    skip_signature = not (
        os.environ.get("AWS_ACCESS_KEY_ID") and os.environ.get("AWS_SECRET_ACCESS_KEY")
    )

    store = obstore.store.S3Store(bucket, region=region, skip_signature=skip_signature)
    return store


@dataclass
class S3Obj:
    bucket: str
    path: str
    size_b: int
    last_modified: datetime


def add_vector_assets(
    stac_item,
    assets: list[S3Obj],
    gpq_schema: str | None,
):
    media_types = {
        "geojson": pystac.MediaType.GEOJSON,
        "parquet": "application/vnd.apache.parquet",
        "gpkg": pystac.MediaType.GEOPACKAGE,
        "shp": "application/vnd.shp",
        "csv": pystac.MediaType.TEXT,
        "json": pystac.MediaType.JSON,
        "txt": pystac.MediaType.TEXT,
        "html": pystac.MediaType.HTML,
        "md": "text/markdown",
    }
    for asset in assets:
        key = asset.path
        size = asset.size_b
        filename = key.rsplit("/", 1)[-1]
        if "." not in filename:
            continue
        name, ext = filename.rsplit(".", 1)

        mtype = media_types.get(ext)
        if not mtype:
            continue

        base_url = f"https://{asset.bucket}.s3.amazonaws.com/"
        href = base_url + key.replace(asset.bucket, "")
        extra_fields = {
            "alternate": {
                "s3": {"title": "s3 URI", "href": f"s3://{asset.bucket}{key}"}
            },
            "last_modified": asset.last_modified.isoformat(timespec="seconds").replace(
                "+00:00", "Z"
            ),
        }
        if ext == "parquet" and gpq_schema:
            extra_fields["geoparquet"] = gpq_schema

        asset_obj = pystac.Asset(href=href, media_type=mtype)
        asset_obj.extra_fields = extra_fields

        stac_item.add_asset(f"{name}_{ext}", asset_obj)

        file_ext = file.FileExtension.ext(asset_obj, True)
        file_ext.apply(size=size)


def clean_s3_path(path: str) -> str:
    if "://" in path:
        # Remove scheme and bucket
        clean = path.split("://", 1)[-1]
        # Remove bucket name (first segment)
        clean = "/".join(clean.split("/")[1:])
        return clean
    else:
        # Path is already clean (no scheme, no bucket)
        return path


def get_aws_mdata(
    path: str, store: obstore.store.S3Store, is_dir: bool = False
) -> list[S3Obj]:
    clean_path = clean_s3_path(path)
    if is_dir:
        items = store.list(clean_path).collect()
        return [
            S3Obj(
                bucket=store.config["bucket"],
                last_modified=obj["last_modified"],
                path=obj["path"],
                size_b=obj["size"],
            )
            for obj in items
        ]

    aws_head = store.head(clean_path)
    return [
        S3Obj(
            bucket=store.config["bucket"],
            last_modified=aws_head["last_modified"],
            path=clean_path,
            size_b=aws_head["size"],
        )
    ]
