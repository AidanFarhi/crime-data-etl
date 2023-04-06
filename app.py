import os
import boto3
import snowflake.connector
import pandas as pd
import json
from datetime import date
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv
load_dotenv()


def get_df_from_s3(client, bucket_name, extract_date):
	objects_metadata = client.list_objects(
		Bucket=bucket_name, Prefix=f'real_estate/crime/{extract_date}'
	)
	keys = [obj['Key'] for obj in objects_metadata['Contents']]
	objects = [client.get_object(Bucket=bucket_name, Key=key) for key in keys]
	result = pd.DataFrame()
	for obj in objects:
		d = json.loads(obj['Body'].read().decode('utf-8'))
		zip_code = d['Overall']['Zipcode']
		crimes =  d['Crime BreakDown']
		vals_to_put_in_df = []
		for crime_obj in crimes:
			key = list(filter(lambda x: 'Crime Rates' in x, crime_obj.keys()))[0]
			crime_set = crime_obj[key]
			for k, v in crime_set.items():
				vals_to_put_in_df.append({
					'ZIP_CODE': zip_code, 'CRIME_TYPE': k.upper(), 'RATE': float(v), 'AS_OF_DATE': str(date.today())
				})
		result = pd.concat([result, pd.DataFrame(vals_to_put_in_df)])
	return result


def main(event, context):	
	bucket_name = os.getenv('BUCKET_NAME')
	client = boto3.client(
		's3', 
		endpoint_url='https://s3.amazonaws.com',
		aws_access_key_id=os.getenv('ACCESS_KEY'),
		aws_secret_access_key=os.getenv('SECRET_ACCESS_KEY')
	)
	conn = snowflake.connector.connect(
		user=os.getenv('SNOWFLAKE_USERNAME'),
		password=os.getenv('SNOWFLAKE_PASSWORD'),
		account=os.getenv('SNOWFLAKE_ACCOUNT'),
		warehouse=os.getenv('WAREHOUSE'),
		database=os.getenv('DATABASE'),
		schema=os.getenv('SCHEMA')
	)
	extract_date = event['extractDate']

	# Get data from S3
	crime_rate_df = get_df_from_s3(client, bucket_name, extract_date)

	# Load to Snowflake
	write_pandas(conn, crime_rate_df, 'DIM_CRIME_RATE')
	return {'statusCode': 200}
