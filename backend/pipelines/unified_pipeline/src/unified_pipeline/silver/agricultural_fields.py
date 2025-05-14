import asyncio
import json

import geopandas as gpd
import pandas as pd

from unified_pipeline.common.base import BaseJobConfig, BaseSource
from unified_pipeline.util.gcs_util import GCSUtil
from unified_pipeline.util.geometry_validator import validate_and_transform_geometries
from unified_pipeline.util.timing import AsyncTimer


class AgriculturalFieldsSilverConfig(BaseJobConfig):
    fields_dataset: str = "agricultural_fields"
    blocks_dataset: str = "agricultural_blocks"
    bucket: str = "landbrugsdata-raw-data"
    storage_batch_size: int = 5000
    column_mapping: dict[str, str] = {
        "Marknr": "field_id",
        "IMK_areal": "area_ha",
        "Journalnr": "journal_number",
        "CVR": "cvr_number",
        "Afgkode": "crop_code",
        "Afgroede": "crop_type",
        "GB": "organic_farming",
        "GBanmeldt": "reported_area_ha",
        "Markblok": "block_id",
        "MB_NR": "block_id",
        "BLOKAREAL": "block_area_ha",
        "MARKBLOKTY": "block_type",
    }


class AgriculturalFieldsSilver(BaseSource[AgriculturalFieldsSilverConfig]):
    def __init__(self, config: AgriculturalFieldsSilverConfig, gcs_util: GCSUtil):
        super().__init__(config, gcs_util)

    async def extract_geojson_from_payload(
        self, payload_json: str, column_mapping: dict
    ) -> gpd.GeoDataFrame:
        try:
            payload = json.loads(payload_json)
            features = payload.get("features", [])

            # Convert features to GeoJSON format
            geojson_features = []
            for feature in features:
                geojson_feature = {
                    "type": "Feature",
                    "properties": feature["attributes"],
                    "geometry": {"type": "Polygon", "coordinates": feature["geometry"]["rings"]},
                }
                geojson_features.append(geojson_feature)

            if geojson_features:
                geo_df = gpd.GeoDataFrame.from_features(geojson_features, crs="EPSG:25832")
                geo_df = geo_df.rename(columns=column_mapping)
                return geo_df  # type: ignore[no-any-return]
            else:
                return gpd.GeoDataFrame()
        except Exception as e:
            self.log.error(f"Error parsing payload: {e}")
            return gpd.GeoDataFrame()

    async def _process_data(self, raw_df: pd.DataFrame, dataset: str) -> gpd.GeoDataFrame:
        async with AsyncTimer("Processing data"):
            payloads = raw_df["payload"].tolist()
            tasks = [
                self.extract_geojson_from_payload(payload, self.config.column_mapping)
                for payload in payloads
            ]
            geo_dfs_list = await asyncio.gather(*tasks)
            # Filter out empty dataframes
            geo_dfs_list = [gdf for gdf in geo_dfs_list if not gdf.empty]

            if not geo_dfs_list:
                return gpd.GeoDataFrame()
            geo_df = gpd.GeoDataFrame(pd.concat(geo_dfs_list, ignore_index=True))

            geo_df.columns = [
                col.replace(".", "_").replace("()", "_").replace("(", "_").replace(")", "_")
                for col in geo_df.columns
            ]

            # Validate and transform geometries
            geo_df = validate_and_transform_geometries(geo_df, dataset)

            return geo_df

    async def run(self) -> None:
        self.log.info("Running Agricultural Fields silver job")
        async with AsyncTimer("Agricultural Fields Silver Job"):
            for dataset in [self.config.fields_dataset, self.config.blocks_dataset]:
                raw_data = self._read_bronze_data(dataset, self.config.bucket)
                if raw_data is None:
                    self.log.error("Failed to read raw data")
                    return
                self.log.info("Read raw data successfully")
                geo_df = await self._process_data(raw_data, dataset)
                if geo_df is None:
                    self.log.error("Failed to process raw data")
                    return
                self.log.info("Processed raw data successfully")
                self._save_data(geo_df, dataset, self.config.bucket)
                self.log.info("Saved processed data successfully")
