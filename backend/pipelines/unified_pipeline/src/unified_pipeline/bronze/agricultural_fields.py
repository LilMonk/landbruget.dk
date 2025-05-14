import asyncio
import json
import ssl
from asyncio import Semaphore

import aiohttp
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
    fields_dataset: str = "agricultural_fields"
    blocks_dataset: str = "agricultural_blocks"
    frequency: str = "weekly"
    bucket: str = "landbrugsdata-raw-data"

    batch_size: int = 20000
    max_concurrent: int = 5
    storage_batch_size: int = 10000
    max_retries: int = 3

    timeout_config: aiohttp.ClientTimeout = aiohttp.ClientTimeout(
        total=1200, connect=60, sock_read=540
    )
    request_semaphore: Semaphore = Semaphore(max_concurrent)

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)


class AgriculturalFieldsBronze(BaseSource[AgriculturalFieldsBronzeConfig]):
    def __init__(self, config: AgriculturalFieldsBronzeConfig, gcs_util: GCSUtil):
        super().__init__(config, gcs_util)

    async def _get_total_count(self, session: aiohttp.ClientSession, url: str) -> int:
        """Get total number of features for a specific endpoint"""
        params = {"f": "json", "where": "1=1", "returnCountOnly": "true"}

        try:
            self.log.info(f"Fetching total count from {url}")
            async with session.get(url, params=params) as response:
                async with AsyncTimer(f"Request total count from {url}"):
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
            async with AsyncTimer(f"Processing data for {dataset}"):
                total_count = await self._get_total_count(session, url)
                self.log.info(f"Total count: {total_count}")

                if total_count == 0:
                    self.log.warning("No data to process.")
                    return

                tasks = []
                for start_index in range(0, total_count, self.config.batch_size):
                    tasks.append(self._fetch_chunk(session, url, start_index))

                raw_data = await asyncio.gather(*tasks)
                self.log.info(f"Saving data to GCS for {dataset}")
                self._save_raw_data(raw_data, dataset, self.config.name, self.config.bucket)
                self.log.info(f"Data processing completed for {dataset}")

    async def run(self) -> None:
        """
        Run the data source processing pipeline.

        This method fetches data from the configured URLs, processes it, and saves it
        to Google Cloud Storage.

        Returns:
            None
        """
        self.log.info("Running Agricultural Fields bronze job")
        async with AsyncTimer("Total run time"):
            await self._process_data(self.config.fields_url, self.config.fields_dataset)
            await self._process_data(self.config.blocks_url, self.config.blocks_dataset)

            self.log.info("Run completed successfully.")
