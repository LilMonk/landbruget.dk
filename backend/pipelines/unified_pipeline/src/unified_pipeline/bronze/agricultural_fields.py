import asyncio
import json
import os
import ssl
from asyncio import Semaphore

import aiohttp
import pandas as pd
from pydantic import ConfigDict
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from unified_pipeline.common.base import BaseJobConfig, BaseSource
from unified_pipeline.util.gcs_util import GCSUtil
from unified_pipeline.util.timing import AsyncTimer


class AgriculturalFieldsBronzeConfig(BaseJobConfig):
    """
    Configuration for the Agricultural Fields Bronze source.
    """

    name: str = "Danish Agricultural Fields"
    type: str = "arcgis"
    description: str = "Weekly updated agricultural field data"
    fields_url: str = (
        "https://kort.vd.dk/server/rest/services/Grunddata/Marker_og_Markblokke/MapServer/13/query"
    )
    blocks_url: str = (
        "https://kort.vd.dk/server/rest/services/Grunddata/Marker_og_Markblokke/MapServer/6/query"
    )
    frequency: str = "weekly"
    enabled: bool = True
    bucket: str = "rahul_apeability"

    batch_size: int = 20000
    max_concurrent: int = 5
    storage_batch_size: int = 10000
    max_retries: int = 3

    timeout_config: aiohttp.ClientTimeout = aiohttp.ClientTimeout(
        total=1200, connect=60, sock_read=540
    )
    request_semaphore: Semaphore = Semaphore(max_concurrent)
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

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)


class AgriculturalFieldsBronze(BaseSource[AgriculturalFieldsBronzeConfig]):
    def __init__(self, config: AgriculturalFieldsBronzeConfig, gcs_util: GCSUtil):
        super().__init__(config, gcs_util)
        self.config = config

    async def _get_total_count(self, session: aiohttp.ClientSession, url: str) -> int:
        """Get total number of features for a specific endpoint"""
        params = {"f": "json", "where": "1=1", "returnCountOnly": "true"}

        try:
            self.log.info(f"Fetching total count from {url}")
            async with session.get(url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    total = data.get("count", 0)
                    return int(total)
                else:
                    response_text = await response.text()
                    raise Exception(
                        f"Error getting count for {url}: {response.status} - {response_text}"
                    )
        except Exception as e:
            raise Exception(f"Error getting total count for {url}: {str(e)}")

    @retry(
        retry=retry_if_exception_type(Exception),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        stop=stop_after_attempt(5),
    )
    async def _fetch_chunk(self, session: aiohttp.ClientSession, url: str, start_index: int) -> str:
        """Fetch a chunk of features with retry logic"""
        params = {
            "f": "json",
            "where": "1=1",
            "returnGeometry": "true",
            "outFields": "*",
            "resultOffset": str(start_index),
            "resultRecordCount": str(self.config.batch_size),
        }

        async with self.config.request_semaphore:
            async with AsyncTimer(f"Request chunk at index {start_index}"):
                self.log.debug(f"Fetching from URL: {url} with params: {params}")
                async with session.get(url, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        return json.dumps(data)

                    response_text = await response.text()
                    err_msg = (
                        f"Error response {response.status} at index {start_index}. "
                        f"Response: {response_text[:500]}..."
                    )
                    self.log.error(err_msg)
                    raise Exception(err_msg)

    async def _save_raw_data(self, raw_data: list[str], dataset: str) -> None:
        """
        Save raw JSON data to Google Cloud Storage.

        This method creates a DataFrame with the raw JSON data and metadata,
        saves it as a parquet file locally, then uploads it to Google Cloud Storage.

        Args:
            raw_data (list[str]): A list of JSON strings to save.
            dataset (str): The name of the dataset, used to determine the save path.

        Returns:
            None

        Raises:
            Exception: If there are issues saving the data.

        Note:
            The data is saved in the bronze layer, which contains raw, unprocessed data.
            The file is named with the current date in YYYY-MM-DD format.
        """
        self.log.info(f"Saving raw data for {dataset} to GCS")
        bucket = self.gcs_util.get_gcs_client().bucket(self.config.bucket)
        df = pd.DataFrame(
            {
                "payload": raw_data,
            }
        )
        df["source"] = self.config.name
        df["created_at"] = pd.Timestamp.now()
        df["updated_at"] = pd.Timestamp.now()

        temp_dir = f"/tmp/bronze/{dataset}"
        os.makedirs(temp_dir, exist_ok=True)
        current_date = pd.Timestamp.now().strftime("%Y-%m-%d")
        temp_file = f"{temp_dir}/{current_date}.parquet"
        working_blob = bucket.blob(f"bronze/{dataset}/{current_date}.parquet")

        df.to_parquet(temp_file)
        working_blob.upload_from_filename(temp_file)
        self.log.info(
            f"Uploaded to: gs://{self.config.bucket}/bronze/{dataset}/{current_date}.parquet"
        )

    async def _process_data(self, url: str, dataset: str) -> None:
        """
        Process data from the specified URL and save it to Google Cloud Storage.
        This method fetches data in chunks, processes it, and saves it as a parquet file.
        Args:
            url (str): The URL to fetch data from.
            dataset (str): The name of the dataset, used to determine the save path.
        Returns:
            None
        Raises:
            Exception: If there are issues processing the data.
        """

        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE

        connector = aiohttp.TCPConnector(ssl=ssl_context)
        async with aiohttp.ClientSession(
            timeout=self.config.timeout_config, connector=connector
        ) as session:
            total_count = await self._get_total_count(session, url)
            self.log.info(f"Total count: {total_count}")

            if total_count == 0:
                self.log.warning("No data to process.")
                return

            tasks = []
            for start_index in range(0, total_count, self.config.batch_size):
                tasks.append(self._fetch_chunk(session, url, start_index))

            raw_data = await asyncio.gather(*tasks)
            await self._save_raw_data(raw_data, dataset)
            self.log.info(f"Data processing completed for {dataset}")

    async def run(self) -> None:
        """
        Run the data source processing pipeline.

        This method fetches data from the configured URLs, processes it, and saves it
        to Google Cloud Storage.

        Returns:
            None
        """
        if not self.config.enabled:
            self.log.warning("Source is disabled. Skipping run.")
            return

        await self._process_data(self.config.fields_url, "agricultural_fields")
        await self._process_data(self.config.blocks_url, "agricultural_blocks")
        self.log.info("Data processing completed for all datasets")
        self.log.info("Run completed successfully.")
