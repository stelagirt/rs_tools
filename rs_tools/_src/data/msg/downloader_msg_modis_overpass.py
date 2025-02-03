import typer
import tqdm
import autoroot
from loguru import logger
from datetime import datetime, timedelta
import numpy as np
import pandas as pd

from pathlib import Path
from dataclasses import dataclass
from typing import List

from rs_tools import msg_download
from rs_tools._src.data.modis import _check_earthdata_login, modis_granule_to_datetime, query_modis_timestamps
from rs_tools._src.data.msg import MSGFileName
from rs_tools._src.data.modis.downloader_aqua_day import MODISAquaDownload
from rs_tools._src.data.modis.downloader_terra_day import MODISTerraDownload

@dataclass
class MSGDownload:
    """Downloading class for MSG data and cloud mask"""
    predefined_timestamps: List[str]
    save_dir: str 

    def download(self) -> List[str]:
        msg_files = msg_download(
            predefined_timestamps=self.predefined_timestamps,
            satellite="MSG",
            save_dir=Path(self.save_dir).joinpath("L1b"),
            instrument="HRSEVIRI",
            processing_level='L1',
        )
        return msg_files
    
    def download_cloud_mask(self) -> List[str]:
        msg_files = msg_download(
            predefined_timestamps=self.predefined_timestamps,
            satellite="MSG",
            save_dir=Path(self.save_dir).joinpath("CM"),
            instrument="CLM",
            processing_level='L1',
        )
        return msg_files
    

def download(
        modis_product: str="MOD021KM", # TODO: Add advanced data product mapping
        start_date: str="2023-06-01",
        end_date: str="2023-09-30",
        start_time: str="00:00:00", 
        end_time: str="23:59:00", 
        save_dir: str='/mnt/data8tb/fire_detection', 
        cloud_mask: bool = True,
        fire_mask: bool = True
):
    """
    Downloads MSG data including cloud mask 

    Args:
        modis_product (str): The MODIS product to download (default: 'MYD021KM', i.e. Aqua at 1km resolution)
        start_date (str): The start date of the data to download (format: 'YYYY-MM-DD')
        end_date (str): The end date of the data to download (format: 'YYYY-MM-DD')
        start_time (str): The start time of the data to download (format: 'HH:MM:SS')
        end_time (str): The end time of the data to download (format: 'HH:MM:SS')
        save_dir (str): The path to save the downloaded data
        cloud_mask (bool, optional): Whether to download the cloud mask data (default: True)
        fire_mask (bool, optional): Whether to download the fire mask data (default: True)

    Returns:
        List[str]: List of downloaded file names
    """

    logger.info("Querying MODIS overpasses for MSG field-of-view and specified time period...")
    # Check EartData login
    _check_earthdata_login()
    # Compile MODIS timestamp tuple
    start_datetime_str = start_date + ' ' + start_time
    end_datetime_str = end_date + ' ' + end_time
    # Query MODIS timestamps
    modis_results = query_modis_timestamps(
        short_name=modis_product,
        bounding_box=(-10.019531, 30.22889, 46.617188, 49.012224), # I changed the AOI to North Africa -- South Europe -- TODO add it to beginning as command line argument (Stella)
        temporal=(start_datetime_str, end_datetime_str)
    )
    logger.info(f"Found {len(modis_results)} MODIS granules for MSG field-of-view and specified time period...")
    # Extract MODIS timestamps
    modis_timestamps = [modis_granule_to_datetime(x) for x in modis_results]

     # **Download MODIS Data using AQUA Downloader**
    modis_save_path = Path(save_dir).joinpath("modis")
    modis_save_path.mkdir(parents=True, exist_ok=True)

    logger.info("Initializing AQUA/MODIS Downloader...")
    aqua_downloader = MODISAquaDownload(
        start_date=start_date,
        end_date=end_date,
        start_time=start_time,
        end_time=end_time,
        save_dir=str(modis_save_path),
        bounding_box=(-10.019531, 30.22889, 46.617188, 49.012224),  # I changed the AOI to North Africa -- South Europe -- TODO add it to beginning as command line argument (Stella)
    )

    terra_downloader = MODISTerraDownload(
        start_date=start_date,
        end_date=end_date,
        start_time=start_time,
        end_time=end_time,
        save_dir=str(modis_save_path),
        bounding_box=(-10.019531, 30.22889, 46.617188, 49.012224),  # I changed the AOI to North Africa -- South Europe -- TODO add it to beginning as command line argument (Stella)
    )

    if modis_product.startswith("MYD"):
        modis_downloader = aqua_downloader
    elif modis_product.startswith("MOD"):
        modis_downloader = terra_downloader

    logger.info("Downloading MODIS data...")
    modis_filenames = modis_downloader.download()

    if cloud_mask:
        logger.info("Downloading AQUA Cloud Mask...")
        modis_filenames = modis_downloader.download_cloud_mask()
        logger.info("Done!")
    if fire_mask:
        logger.info("Downloading AQUA Fire Mask...")
        modis_filenames = modis_downloader.download_fire_mask()
        logger.info("Done!")

    # Initialize MSG Downloader
    logger.info("Initializing MSG Downloader...")

    dc_msg_download = MSGDownload(
        predefined_timestamps=modis_timestamps,
        save_dir=Path(save_dir).joinpath("msg"),
    )
    logger.info("Downloading MSG Data...")
    msg_filenames, msg_queries = dc_msg_download.download()
    logger.info("Done!")

    if cloud_mask:
        logger.info("Downloading MSG Cloud Mask...")
        msg_cm_filenames, msg_cm_queries = dc_msg_download.download_cloud_mask()
        logger.info("Done!")

    logger.info("Finished Data Download...")

    if len(msg_filenames) > 0: # Check if that files were downloade=
        assert len(msg_filenames) == len(msg_queries), "Mismatch between queries and downloaded files"
        list_modis_times = msg_queries
        list_msg_times = [str(MSGFileName.from_filename(msg_filenames[x]).datetime_acquisition) for x in range(len(msg_filenames))]

        # Compile summary file
        logger.info("Compiling Summary File...")
        df = pd.DataFrame()
        df['MODIS'] = list_modis_times
        df['MSG'] = list_msg_times

        if cloud_mask:
            assert len(msg_cm_filenames) == len(msg_cm_queries), "Mismatch between queries and downloaded files"
            list_msg_cm_times = [str(MSGFileName.from_filename(msg_cm_filenames[x]).datetime_acquisition) for x in range(len(msg_cm_filenames))]
            try: # TODO: Add fix is length of cloud mask timestamps is not equal to MSG timestamps
                df['MODIS_cloudmask'] = msg_cm_queries
                df['MSG_cloudmask'] = list_msg_cm_times
            except Exception as e:
                logger.error(f"Error: {e}")
                logger.error("Could not add cloud mask timestamps to summary file...")

        df.to_csv(Path(save_dir).joinpath(f"msg-{modis_product}-timestamps_{start_date}_{end_date}.csv"), index=False)

    logger.info("Done!")
    logger.info("Finished MSG Downloading Script...")

if __name__ == '__main__':
    typer.run(download)