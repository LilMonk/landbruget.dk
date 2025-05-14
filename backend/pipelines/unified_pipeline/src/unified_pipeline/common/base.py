"""
Base classes for data sources in the unified pipeline.

This module defines the abstract base classes that all data sources in
the unified pipeline must implement. It provides common functionality and
enforces a consistent interface across different data sources and stages.
"""

import os
from abc import ABC, abstractmethod
from typing import Generic, Optional, TypeVar

import geopandas as gpd
import pandas as pd
from pydantic import BaseModel

from unified_pipeline.util.gcs_util import GCSUtil
from unified_pipeline.util.log_util import Logger


class BaseJobConfig(BaseModel):
    """
    Base configuration model for all data sources.

    This class defines common configuration properties that all data sources
    share and serves as a foundation for source-specific configuration models.
    It uses Pydantic for validation and type checking.

    All specific source configurations should inherit from this class and
    add their own specific configuration parameters.

    Example:
        >>> class MySourceConfig(BaseJobConfig):
        >>>     input_path: str
        >>>     output_bucket: str
    """


T = TypeVar("T", bound=BaseJobConfig)


class BaseSource(Generic[T], ABC):
    """
    Abstract base class for all data sources in the unified pipeline.

    This class defines the common interface and shared functionality that
    all data sources must implement. It handles configuration management,
    logging, and access to GCS utilities.

    Type parameter:
        T: Configuration type that extends BaseJobConfig

    Attributes:
        config: Source-specific configuration object
        gcs_util: Google Cloud Storage utility instance
        log: Logger instance for this source
    """

    def __init__(self, config: T, gcs_util: GCSUtil) -> None:
        """
        Initialize a new data source.

        Args:
            config: Source-specific configuration object
            gcs_util: Google Cloud Storage utility instance for cloud storage operations
        """
        self.config = config
        self.gcs_util = gcs_util
        self.log = Logger.get_logger()

    @abstractmethod
    async def run(self) -> None:
        """
        Run the data source processing pipeline.

        This method must be implemented by all concrete source classes.
        It should handle the entire process of fetching, transforming, and
        storing data according to the source's specific requirements.

        Returns:
            None

        Raises:
            NotImplementedError: If the concrete class does not implement this method
        """
        pass

    def _save_raw_data(
        self, raw_data: list[str], dataset: str, source_name: str, bucket_name: str
    ) -> None:
        """
        Save raw data to Google Cloud Storage.

        This method creates a DataFrame with the raw data and metadata,
        saves it as a parquet file locally, then uploads it to Google Cloud Storage.

        Args:
            raw_data (list[str]): A list of strings to save.
            dataset (str): The name of the dataset, used to determine the save path.
            source_name (str): The name of the source, used for logging and metadata.
            bucket_name (str): The name of the GCS bucket to save the data.

        Returns:
            None

        Raises:
            Exception: If there are issues saving the data.

        Note:
            The data is saved in the bronze layer, which contains raw, unprocessed data.
            The file is named with the current date in YYYY-MM-DD format.
        """
        bucket = self.gcs_util.get_gcs_client().bucket(bucket_name)
        df = pd.DataFrame(
            {
                "payload": raw_data,
            }
        )
        df["source"] = source_name
        df["created_at"] = pd.Timestamp.now()
        df["updated_at"] = pd.Timestamp.now()

        temp_dir = f"/tmp/bronze/{dataset}"
        os.makedirs(temp_dir, exist_ok=True)
        current_date = pd.Timestamp.now().strftime("%Y-%m-%d")
        temp_file = f"{temp_dir}/{current_date}.parquet"
        working_blob = bucket.blob(f"bronze/{dataset}/{current_date}.parquet")

        df.to_parquet(temp_file)
        working_blob.upload_from_filename(temp_file)
        self.log.info(f"Uploaded to: gs://{bucket_name}/bronze/{dataset}/{current_date}.parquet")
        return

    def _save_data(self, df: gpd.GeoDataFrame, dataset: str, bucket_name: str) -> None:
        """
        Save processed data to Google Cloud Storage.

        This method saves a GeoDataFrame to GCS as a parquet file. It creates
        a temporary local file and then uploads it to the specified GCS bucket.

        Args:
            df (gpd.GeoDataFrame): The GeoDataFrame to save.
            dataset (str): The name of the dataset, used to determine the save path.
            bucket_name (str): The name of the GCS bucket to save the data.

        Returns:
            None

        Raises:
            Exception: If there are issues saving the data.
        """
        if df is None or df.empty:
            self.log.warning("No processed data to save")
            return

        self.log.info(f"Saving processed data to GCS: records: {df.shape[0]:,}")
        bucket = self.gcs_util.get_gcs_client().bucket(bucket_name)

        temp_dir = f"/tmp/silver/{dataset}"
        os.makedirs(temp_dir, exist_ok=True)
        current_date = pd.Timestamp.now().strftime("%Y-%m-%d")
        temp_file = f"{temp_dir}/{current_date}.parquet"
        working_blob = bucket.blob(f"silver/{dataset}/{current_date}.parquet")

        df.to_parquet(temp_file)
        working_blob.upload_from_filename(temp_file)
        self.log.info(f"Uploaded to: gs://{bucket_name}/silver/{dataset}/{current_date}.parquet")

    def _read_bronze_data(self, dataset: str, bucket_name: str) -> Optional[pd.DataFrame]:
        """
        Read data from the bronze layer.

        This method retrieves data from the bronze layer in Google Cloud Storage.
        It downloads the parquet file for the current date and loads it into a DataFrame.

        Args:
            dataset (str): The name of the dataset to read.
            bucket_name (str): The name of the GCS bucket.

        Returns:
            Optional[pd.DataFrame]: A DataFrame containing the bronze layer data,
                                    or None if no data is found.

        Raises:
            Exception: If there are issues accessing or downloading the data.
        """
        self.log.info("Reading data from bronze layer")

        # Get the GCS bucket
        bucket = self.gcs_util.get_gcs_client().bucket(bucket_name)

        # Define the path to the bronze data
        current_date = pd.Timestamp.now().strftime("%Y-%m-%d")
        bronze_path = f"bronze/{dataset}/{current_date}.parquet"
        blob = bucket.blob(bronze_path)

        if not blob.exists():
            self.log.error(f"Bronze data not found at {bronze_path}")
            return None

        # Download to temporary file
        temp_dir = f"/tmp/bronze/{dataset}"
        os.makedirs(temp_dir, exist_ok=True)
        temp_file = f"{temp_dir}/{current_date}.parquet"
        blob.download_to_filename(temp_file)

        # Load the parquet file
        raw_data = pd.read_parquet(temp_file)
        self.log.info(f"Loaded {len(raw_data):,} records from bronze layer")

        return raw_data
