import pandas as pd
import boto3
import logging
import json
import tempfile

import io,os
from botocore.exceptions import ClientError
from boto3.s3.transfer import TransferConfig
from boto3.dynamodb.conditions import Key
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as Fsum, last_day, trunc, to_date, expr
from delta.tables import DeltaTable
from pyspark import StorageLevel


logging.getLogger().setLevel(logging.INFO)
class S3ConnectionMR:
    def __init__(self, bucket=None, client_secret=None, base_path = None):
        self.bucket = bucket
        self.s3_client = self.get_client(client_secret)
        self.base_path = base_path

    @staticmethod
    def get_client(client_secret=None):
        if client_secret:
            try:
                with open(client_secret) as f:
                    data = json.load(f)

                    ACCESS_KEY = data.get("ACCESS_KEY")
                    SECRET_KEY = data.get("SECRET_KEY")

                    s3 = boto3.client(
                        "s3",
                        aws_access_key_id=ACCESS_KEY,
                        aws_secret_access_key=SECRET_KEY,
                        verify=False,
                    )
                    return s3
            except:
                logging.error("Invalid crediential, s3 client not loaded!")

        else:
            try:
                s3 = boto3.client("s3")
                return s3
            except:
                logging.error("Could not connect to s3 with default credentials")

    def read_from_s3(self, filename, engine=None, nrows=None, dtype=None, usecols=None, chunksize=None, encoding='utf-8', header=True, persistence=StorageLevel.DISK_ONLY):
        """
        Parámetros:
        - persistence (StorageLevel, opcional): Nivel de persistencia para PySpark (por defecto DISK_ONLY).
                MEMORY_AND_DISK, DISK_ONLY, MEMORY_ONLY_SER
        """
        response = self.s3_client.get_object(Bucket=self.bucket, Key=filename)

        if engine is None:
            return pd.read_csv(response.get("Body"), encoding=encoding, nrows=nrows, dtype=dtype, usecols=usecols, chunksize=chunksize)

        elif isinstance(engine, SparkSession):
            temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".parquet")
            temp_file.write(response.get("Body").read())
            temp_file.close()

            df = engine.read.parquet(temp_file.name)
            df = df.persist(persistence)

            return df
        else:
            raise ValueError("Invalid engine type. Use None for pandas or pass a SparkSession object for PySpark.")
        
    def load_from_s3(self, filename, nrows=None):
        response = self.s3_client.get_object(Bucket=self.bucket, Key=filename)

        return response.get("body")

    def df_to_s3(self, df=None, key=None, format = 'csv'):
        if format == 'csv':
            buffer = io.StringIO()
            df.to_csv(buffer, index=False)
        elif format == 'parquet':
            buffer = io.BytesIO()
            df.to_parquet(buffer, index=False)
        else:
            raise ValueError("Unsupported format. Use 'csv' or 'parquet'.")
        
        buffer.seek(0)
        self.s3_client.put_object(Body=buffer.getvalue(), Bucket=self.bucket, Key=key)
        
        logging.info(f"File with {df.shape[0]} rows was written to {key}")

    def s3_find_csv(self, path=None, suffix="csv"):
        objects = self.s3_client.list_objects_v2(Bucket=self.bucket)["Contents"]

        return [
            obj["Key"] for obj in objects if path in obj["Key"] and suffix in obj["Key"]
        ]
        
    def s3_load_file(self, key=None) -> object:
        try:
            response = self.s3_client.get_object(Bucket=self.bucket, Key=key)
            logging.info(f"key '{self.bucket}/{key}' has been dowloaded from S3!")
            return response["Body"].read()
        except:
            logging.error(
                f"The key '{self.bucket}/{key}' was not downloaded make sure the file exists."
            )

    def s3_upload(self, file, key, config=None) -> None:
        """
        Upload an object to S3
        """
        try:
                self.s3_client.upload_file(file, self.bucket, Key=key, Config=config)
        except:
            logging.error(f"The file {file} could not be uploaded to {self.bucket}")

    def s3_upload_df(self, df, key, config=None) -> None:

        try:
            with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as tmp_file:
                df.to_csv(tmp_file.name, index=False)
                transfer_config = TransferConfig(
                    multipart_threshold=1024 * 100,  # Default 25MB
                    max_concurrency=10,  # Default 10 threads
                    multipart_chunksize=1024 * 100,  # Default 25MB per part
                    use_threads=True  # Default use threading
                )
                
                self.s3_client.upload_file(tmp_file.name, self.bucket, key, Config=transfer_config)
                
        except ClientError as e:
            logging.error(f"The DataFrame could not be uploaded to {self.bucket}/{key}", exc_info=True)


    def s3_download(self, file, key):
        # try:

        self.s3_client.download_file(self.bucket, key, file)


import boto3
import json
import logging
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key

class DynamoDBConnection:
    def __init__(self, client_secret="./secret/client_secret.json"):
        self.dynamodb = self.get_client(client_secret)
        self.table = None

    @staticmethod
    def get_client(client_secret="client_secret.json"):
        try:
            with open(client_secret) as f:
                data = json.load(f)
                ACCESS_KEY = data.get("ACCESS_KEY")
                SECRET_KEY = data.get("SECRET_KEY")
                
                dynamodb = boto3.resource(
                    'dynamodb',
                    aws_access_key_id=ACCESS_KEY,
                    aws_secret_access_key=SECRET_KEY,
                    region_name='us-east-1'  # Cambia a tu región de AWS
                )
                return dynamodb
        except Exception as e:
            logging.error(f"DynamoDB client not loaded: {str(e)}")
            return None
    
    def connect_table(self, table_name):
        if self.dynamodb:
            self.table = self.dynamodb.Table(table_name)
            logging.info(f"Connected to table {table_name}")
        else:
            logging.error("DynamoDB client is not initialized.")
    
    def put_item(self, item):
        if not self.table:
            logging.error("No table connected.")
            return None
        try:
            response = self.table.put_item(Item=item)
            logging.info(f"Item successfully put in table {self.table.name}")
            return response
        except ClientError as e:
            logging.error(f"Error putting item in table {self.table.name}: {str(e)}")
    
    def get_item(self, key):
        if not self.table:
            logging.error("No table connected.")
            return None
        try:
            response = self.table.get_item(Key=key)
            if 'Item' in response:
                logging.info(f"Item retrieved from {self.table.name}: {response['Item']}")
                return response['Item']
            else:
                logging.warning(f"Item not found in {self.table.name} for key: {key}")
                return None
        except ClientError as e:
            logging.error(f"Error retrieving item from table {self.table.name}: {str(e)}")
            return None
    
    def update_item(self, key, update_expression, expression_values):
        if not self.table:
            logging.error("No table connected.")
            return None
        try:
            response = self.table.update_item(
                Key=key,
                UpdateExpression=update_expression,
                ExpressionAttributeValues=expression_values,
                ReturnValues="UPDATED_NEW"
            )
            logging.info(f"Item updated in {self.table.name}: {response['Attributes']}")
            return response['Attributes']
        except ClientError as e:
            logging.error(f"Error updating item in table {self.table.name}: {str(e)}")
            return None
    
    def delete_item(self, key):
        if not self.table:
            logging.error("No table connected.")
            return None
        try:
            response = self.table.delete_item(Key=key)
            logging.info(f"Item successfully deleted from {self.table.name}")
            return response
        except ClientError as e:
            logging.error(f"Error deleting item from table {self.table.name}: {str(e)}")
    
    def scan_table(self, filter_expression=None, expression_values=None, limit=None):
        if not self.table:
            logging.error("No table connected.")
            return []
        try:
            scan_params = {}
            if filter_expression:
                scan_params['FilterExpression'] = filter_expression
                scan_params['ExpressionAttributeValues'] = expression_values
            if limit:
                scan_params['Limit'] = limit
            
            response = self.table.scan(**scan_params)
            items = response.get('Items', [])
            logging.info(f"Scanned {len(items)} items from {self.table.name} with a limit of {limit}")
            return items
        except ClientError as e:
            logging.error(f"Error scanning table {self.table.name}: {str(e)}")
            return []
        
    def query_table(self, partition_key, partition_value):
        if not self.table:
            logging.error("No table connected.")
            return []
        try:
            response = self.table.query(
                KeyConditionExpression=Key(partition_key).eq(partition_value)
            )
            items = response.get('Items', [])
            logging.info(f"Queried {len(items)} items from {self.table.name}")
            return items
        except ClientError as e:
            logging.error(f"Error querying table {self.table.name}: {str(e)}")
            return []
