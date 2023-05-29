import os
import boto3
import snowflake.connector
import pandas as pd
import json
from datetime import date
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv

load_dotenv()


def get_df_from_s3(client: boto3.client, bucket_name: str, extract_date: str) -> pd.DataFrame:
    """
    Retrieve DataFrame from S3 bucket for a given extract date.

    Args:
        client (boto3.client): Boto3 client for S3.
        bucket_name (str): Name of the S3 bucket.
        extract_date (str): Extract date string.

    Returns:
        pd.DataFrame: DataFrame containing the retrieved data.
    """
    objects_metadata = client.list_objects(Bucket=bucket_name, Prefix=f"real_estate/crime/{extract_date}")
    keys = [obj["Key"] for obj in objects_metadata["Contents"]]
    objects = [client.get_object(Bucket=bucket_name, Key=key) for key in keys]
    result = pd.DataFrame()
    for obj in objects:
        d = json.loads(obj["Body"].read().decode("utf-8"))
        zip_code = d["Overall"]["Zipcode"]
        crimes = d["Crime BreakDown"]
        vals_to_put_in_df = []
        for crime_obj in crimes:
            key = list(filter(lambda x: "Crime Rates" in x, crime_obj.keys()))[0]
            crime_set = crime_obj[key]
            for k, v in crime_set.items():
                vals_to_put_in_df.append(
                    {
                        "ZIP_CODE": zip_code,
                        "CRIME_TYPE": k.upper(),
                        "RATE": float(v),
                        "SNAPSHOT_DATE": str(date.today()),
                    }
                )
        result = pd.concat([result, pd.DataFrame(vals_to_put_in_df)])
    return result


def main(event: dict, context: dict) -> dict:
    """
    Main function for processing data from S3, Snowflake, and loading to Snowflake.

    Args:
        event (dict): Event data.
        context (dict): Context data.

    Returns:
        dict: Response status code.
    """
    bucket_name = os.getenv("BUCKET_NAME")
    client = boto3.client(
        "s3",
        endpoint_url="https://s3.amazonaws.com",
        aws_access_key_id=os.getenv("ACCESS_KEY"),
        aws_secret_access_key=os.getenv("SECRET_ACCESS_KEY"),
    )
    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USERNAME"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("WAREHOUSE"),
        database=os.getenv("DATABASE"),
        schema=os.getenv("SCHEMA"),
    )
    extract_date = event["extractDate"]

    # Get data from S3
    crime_rate_df = get_df_from_s3(client, bucket_name, extract_date)

    # Get DE location data from Snowflake
    location_df = pd.read_sql("SELECT location_id, zip_code FROM dim_location WHERE state = 'DE'", conn)

    # Get date_id from dim_date table
    dim_date_df = pd.read_sql(f"SELECT date_id, date FROM dim_date WHERE date = '{date.today()}'", conn)

    # Add location_id to crime_rate data
    crime_rate_merged_df = crime_rate_df.merge(location_df, on="ZIP_CODE", how="inner")

    # Convert snapshot_date column do date type for merge
    crime_rate_merged_df.SNAPSHOT_DATE = pd.to_datetime(crime_rate_merged_df.SNAPSHOT_DATE).dt.date

    # Add snapshot_date_id to crime_rate data
    final_df = crime_rate_merged_df.merge(dim_date_df, left_on="SNAPSHOT_DATE", right_on="DATE", how="inner")

    # Rename date column
    final_df = final_df.rename(columns={"DATE_ID": "SNAPSHOT_DATE_ID"})

    # Filter
    final_df = final_df[["LOCATION_ID", "CRIME_TYPE", "RATE", "SNAPSHOT_DATE_ID"]]

    # Load to Snowflake
    write_pandas(conn, final_df, "FACT_CRIME_RATE")
    return {"statusCode": 200}


if __name__ == "__main__":
    from sys import argv

    main({"extractDate": argv[1]}, None)
