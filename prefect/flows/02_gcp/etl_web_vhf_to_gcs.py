from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from datetime import timedelta
from prefect.tasks import task_input_hash
import numpy as np


@task(retries=3, log_prints=True, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""
    # if randint(0, 1) > 0:
    #     raise Exception

    df = pd.read_csv(dataset_url)

    print(df)
    return df


@task(log_prints=True)
def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues"""
    print(df.columns)
    # df["lpep_pickup_datetime"] = pd.to_datetime(df["lpep_pickup_datetime"])
    df['DOlocationID'].astype(float)

    # df["lpep_dropoff_datetime"] = pd.to_datetime(df["lpep_dropoff_datetime"])
    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df


@task()
def write_local(df: pd.DataFrame,dataset_file: str) -> Path:
    """Write DataFrame out locally as parquet file"""
    path = Path(f"prefect/data/fhv/{dataset_file}.parquet")
    df.to_parquet(path, compression="gzip")
    return path


@task()
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("zoom-gas")
    gcs_block.upload_from_path(from_path=path, to_path=path)
    return 


@flow()
def etl_web_vhf_to_gcs(year:int, month:int) -> None:
    """The main ETL function"""
    # color = "green"
    # year = 2020
    # month = 1
    dataset_file = f"fhv_tripdata_{year}-{month:02}"
    # dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_{year}-{month:02}.csv.gz"
                # https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/
    df = fetch(dataset_url)
    print(df.columns)
    df_clean = clean(df)
    path = write_local(df_clean,dataset_file)
    path = Path(f"prefect/data/fhv/{dataset_file}.parquet")

    # write_gcs(path)


@flow()
def etl_parent_web_vhf_to_gcs() -> None:

    year = 2019
    months = [5,6,7]
    for month in months:
        etl_web_vhf_to_gcs(year,month)


if __name__ == "__main__":
    etl_parent_web_vhf_to_gcs()
