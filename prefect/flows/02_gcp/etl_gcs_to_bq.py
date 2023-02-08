from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials


@task(retries=3,log_prints=True)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Download trip data from GCS"""
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("zoom-gas")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"../data/")
    return Path(f"../data/{gcs_path}")




@task(log_prints=True)
def transform(path: Path, color: str) -> pd.DataFrame:
    """Data cleaning example"""
    df = pd.read_parquet(path)

    if 'tpep_pickup_datetime' in df.columns:
        df.rename(
        columns={"tpep_pickup_datetime": "lpep_pickup_datetime"}, inplace=True)
        df.rename(
        columns={"tpep_dropoff_datetime": "lpep_dropoff_datetime"}, inplace=True)

    df['lpep_pickup_datetime'] = pd.to_datetime(df['lpep_pickup_datetime'])
    df['lpep_dropoff_datetime'] = pd.to_datetime(df['lpep_dropoff_datetime'])   
    # print(
    #     f"pre: missing passenger count: {df['passenger_count'].isna().sum()}")
    # df["passenger_count"].fillna(0, inplace=True)
    # print(
    #     f"post: missing passenger count: {df['passenger_count'].isna().sum()}")
    return df


@task(log_prints=True)
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to BiqQuery"""

    gcp_credentials_block = GcpCredentials.load("zoom-gcp-cred")

    df.to_gbq(
        destination_table="dezoomcamp.rides",
        project_id="data-engineering-camp-376112",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
    )


@flow()
def etl_gcs_to_bq(year: int, month: int, color: str) -> None:
    """Main ETL flow to load data into Big Query"""
    # color = "green"
    # year = 2020
    # month = 1

    path = extract_from_gcs(color, year, month)
    df = transform(path, color)
    write_bq(df)


@flow()
def etl_to_bq_parent_flow(
    months: list[int] = [1, 2], year: int = 2021, color: str = "yellow"
):
    # here
    for month in months:
        etl_gcs_to_bq(year, month, color)


if __name__ == "__main__":
    # color = "green"
    # year = 2020
    # months = [1]
    color = "yellow"
    months = [2, 3]
    year = 2019
    etl_to_bq_parent_flow(months, year, color)
