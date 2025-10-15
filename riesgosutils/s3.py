import pandas as pd
import boto3, logging, json, tempfile, io, os, tempfile
from botocore.exceptions import ClientError,NoCredentialsError
from boto3.s3.transfer import TransferConfig
from boto3.dynamodb.conditions import Key
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as Fsum, last_day, trunc, to_date, expr
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, DateType,TimestampType
)
from delta.tables import DeltaTable
from pyspark import StorageLevel


logging.getLogger().setLevel(logging.INFO)
class S3ConnectionMR:
    def __init__(self, bucket=None, aws_access_key_id=None, aws_secret_access_key=None, client_secret=None, base_path=None):
        self.bucket = bucket
        self.base_path = base_path
        self.s3_client = self.get_client(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            client_secret=client_secret
        )

    @staticmethod
    def get_client(aws_access_key_id=None, aws_secret_access_key=None, client_secret=None):
        # 1. Si se pasan las claves directamente
        if aws_access_key_id and aws_secret_access_key:
            try:
                s3 = boto3.client(
                    "s3",
                    aws_access_key_id=aws_access_key_id,
                    aws_secret_access_key=aws_secret_access_key,
                    verify=False,
                )
                return s3
            except Exception as e:
                logging.error(f"Could not connect to s3 with provided keys: {e}")
        # 2. Si se pasa un path a un archivo JSON
        elif client_secret:
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
            except Exception as e:
                logging.error(f"Invalid credential file, s3 client not loaded! {e}")
        # 3. Si no se pasa nada, usa credenciales por defecto
        else:
            try:
                s3 = boto3.client("s3")
                return s3
            except Exception as e:
                logging.error(f"Could not connect to s3 with default credentials: {e}")
                
    def scan(self, path="", recursive=False):
        """
        Lista archivos y "carpetas" dentro de una ruta S3.
        
        Args:
            path (str): Prefijo o carpeta dentro del bucket.
            recursive (bool): Si True lista todo el árbol recursivamente. Si False solo el primer nivel.

        Returns:
            dict: {'folders': [...], 'files': [...]}
        """
        try:
            paginator = self.s3_client.get_paginator('list_objects_v2')
            result = {'folders': set(), 'files': []}

            operation_parameters = {
                'Bucket': self.bucket,
                'Prefix': path.rstrip('/') + '/' if path and not path.endswith('/') else path,
                'Delimiter': '' if recursive else '/'
            }

            for page in paginator.paginate(**operation_parameters):
                # Carpeta simulada con CommonPrefixes
                if not recursive:
                    for cp in page.get('CommonPrefixes', []):
                        result['folders'].add(cp.get('Prefix'))

                # Archivos
                for obj in page.get('Contents', []):
                    key = obj['Key']
                    if key.endswith('/'):
                        continue  # evitar agregar carpetas como archivos
                    result['files'].append(key)

            result['folders'] = sorted(list(result['folders']))
            result['files'] = sorted(result['files'])
            return result

        except Exception as e:
            logging.error(f"Error scanning S3 path '{path}': {e}")
            return {'folders': [], 'files': []}
        
    def read_from_s3(self, filename, session=None, nrows=None, dtype=None, usecols=None,index_col=None, chunksize=None, encoding='utf-8', sep=",", header=True, persistence=StorageLevel.DISK_ONLY):
        """
        Parámetros:
        - persistence (StorageLevel, opcional): Nivel de persistencia para PySpark (por defecto DISK_ONLY).
                MEMORY_AND_DISK, DISK_ONLY, MEMORY_ONLY_SER
        """
        response = self.s3_client.get_object(Bucket=self.bucket, Key=filename)
        file_extension = filename.split(".")[-1].lower()
    
        python_to_spark = {
            str: StringType(),
            int: IntegerType(),
            float: DoubleType(),
            "string": StringType(),
            "int": IntegerType(),
            "float": DoubleType(),
            "double": DoubleType(),
            "date": DateType(),
            "datetime64[ns]": TimestampType(),
        }
    
        def build_spark_schema(dtypes: dict):
            fields = []
            for col, t in dtypes.items():
                spark_type = python_to_spark.get(t)
                if spark_type is None:
                    raise ValueError(f"Tipo {t} no soportado para la columna {col}")
                fields.append(StructField(col, spark_type, True))
            return StructType(fields)
    
        if file_extension not in ["csv", "parquet"]:
            import warnings
            warnings.warn("Unsupported file format. Only CSV and Parquet are supported.")
            return None
    
        # ----------- CSV -----------
        if file_extension == "csv":
            # Caso Pandas directo (None o str)
            if session is None or isinstance(session, str):
                return pd.read_csv(
                    io.StringIO(response["Body"].read().decode(encoding)),
                    encoding=encoding,
                    nrows=nrows,
                    dtype=dtype,
                    usecols=usecols,
                    chunksize=chunksize,
                    sep=sep,
                    index_col=index_col,
                )
    
            # SparkSession
            if isinstance(session, SparkSession):
                prefix = f"{session.sparkContext.appName.replace(' ', '_')}_{session.sparkContext.applicationId}_"
                temp_file = tempfile.NamedTemporaryFile(delete=False, prefix=prefix, suffix=".csv")
                temp_file.write(response["Body"].read())
                temp_file.close()
    
                schema = build_spark_schema(dtype) if dtype else None
                df = session.read.csv(
                    temp_file.name,
                    header=header,
                    schema=schema,
                    inferSchema=(schema is None),
                )
                if nrows:
                    df = df.limit(nrows)
                return df.persist(persistence)
    
            raise ValueError(
                "Invalid session type. Use None, str or a SparkSession object."
            )
    
        # ----------- PARQUET -----------
        elif file_extension == "parquet":
            # Prefijo para archivo temporal
            if isinstance(session, SparkSession):
                prefix = f"{session.sparkContext.appName.replace(' ', '_')}_{session.sparkContext.applicationId}_"
            elif isinstance(session, str):
                prefix = f"{session}_"
            else:
                prefix = "anonymous_session_"
    
            temp_file = tempfile.NamedTemporaryFile(delete=False, prefix=prefix, suffix=".parquet")
            temp_file.write(response["Body"].read())
            temp_file.close()
    
            # Pandas (None o str)
            if session is None or isinstance(session, str):
                return pd.read_parquet(temp_file.name, engine="pyarrow")
    
            # SparkSession
            if isinstance(session, SparkSession):
                df = session.read.parquet(temp_file.name)
                if dtype:
                    for col, t in dtype.items():
                        spark_type = python_to_spark.get(t)
                        if spark_type:
                            df = df.withColumn(col, df[col].cast(spark_type))
                if nrows:
                    df = df.limit(nrows)
                return df.persist(persistence)
    
            raise ValueError(
                "Invalid session type. Use None, str or a SparkSession object."
            )
                
    def load_from_s3(self, filename, nrows=None):
        response = self.s3_client.get_object(Bucket=self.bucket, Key=filename)

        return response.get("body")

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
    
    def s3_upload_obj(self, file, key, config=None) -> None:
        """
        Upload an object to S3
        """
        try:
                self.s3_client.upload_fileobj(file, self.bucket, Key=key, Config=config)
        except:
            logging.error(f"The file {file} could not be uploaded to {self.bucket}")

    def df_to_s3(self, df=None, key=None, config=None, session=None):
        """
        Upload DataFrame to S3, using direct HTTP upload for CSVs and temp files for parquet
        
        Args:
            df: pandas DataFrame or Spark DataFrame
            key: S3 key (path/filename)
            config: S3 transfer configuration
            engine: SparkSession instance, string identifier, or None
        """
        if hasattr(df, 'toPandas'):  # Check if it's a Spark DataFrame
            logging.info("Converting Spark DataFrame to Pandas DataFrame")
            df = df.toPandas()
        elif not isinstance(df, pd.DataFrame):
            raise ValueError("Input must be either a Pandas DataFrame or Spark DataFrame")

        try:
            # Determine file extension
            file_extension = key.split(".")[-1].lower()
            
            # For CSV files, use direct HTTP upload via StringIO
            if file_extension == 'csv':
                from io import StringIO
                csv_buffer = StringIO()
                df.to_csv(csv_buffer, index=False)
                self.s3_client.put_object(
                    Body=csv_buffer.getvalue(),
                    Bucket=self.bucket,
                    Key=key
                )
                logging.info(f"CSV file with {df.shape[0]} rows was written to {key}")
                return

            elif file_extension == 'parquet':
                
                # Determine prefix based on engine type
                if isinstance(session, SparkSession):
                    app_name = session.sparkContext.appName.replace(" ", "_")
                    app_id = session.sparkContext.applicationId
                    prefix = f"{app_name}_{app_id}_"
                elif isinstance(session, str):
                    prefix = f"{session}_"
                else:
                    prefix = ""

                # Create temporary file
                temp_file = tempfile.NamedTemporaryFile(delete=False, prefix=prefix, suffix=file_extension)
                df.to_parquet(temp_file.name, engine="pyarrow", index=False)

                # Configure transfer
                transfer_config = TransferConfig(
                    multipart_threshold=1024 * 1024 * 25,  # 25MB
                    max_concurrency=10,
                    multipart_chunksize=1024 * 1024 * 25,
                    use_threads=True
                ) if not config else config

                # Upload file
                self.s3_client.upload_file(
                    temp_file.name,
                    self.bucket,
                    key,
                    Config=transfer_config
                )
                logging.info(f"Parquet file was written to {key}")
            else:
                print(f"Unsupported file format [{file_extension}]. Use 'csv' or 'parquet'.")

        except ClientError as e:
            logging.error(f"Error uploading DataFrame to {self.bucket}/{key}: {str(e)}")
            raise
        finally:
            if 'temp_file' in locals():
                os.unlink(temp_file.name)

    def s3_download(self, file, key):
        # try:

        self.s3_client.download_file(self.bucket, key, file)
    
    def s3_presigned_url(self, key, expires= 3600):
        try:
            if not hasattr(self, 's3_client') or not hasattr(self, 'bucket'):
                raise AttributeError("s3_client o bucket no definidos en el objeto.")

            if not key:
                raise ValueError("El parámetro 'key' está vacío o es inválido.")

            presigned_url = self.s3_client.generate_presigned_url(
                'get_object',
                Params={'Bucket': self.bucket, 'Key': key},
                ExpiresIn=expires
            )
            return presigned_url if presigned_url else ''
        except Exception as e:
            print(f"[WARN] No se pudo generar la URL prefirmada: {str(e)}")
            return ''

    def download_file_from_s3(self, s3_path, local_path):
        """Descarga un archivo desde S3 a una ruta local"""
        try:
            self.s3_client.download_file(self.bucket, s3_path, local_path)
            print(f"Descargado: {s3_path} -> {local_path}")
        except Exception as e:
            print(f"Error descargando {s3_path}: {e}")
            raise

    def clear_cache(self,session=None):
        """
        Elimina todos los archivos temporales Parquet generados en el directorio temporal del sistema.
        """
        temp_dir = tempfile.gettempdir()
        if isinstance(session, SparkSession):
            prefix = session.sparkContext.appName.replace(" ", "_")
        elif isinstance(session, str):
            prefix = session
        else:
            prefix = "anonymous_session"

        try:
            for file in os.listdir(temp_dir):
                if file.endswith(".parquet") or file.endswith(".csv"):
                    if prefix:
                        if file.startswith(prefix):
                            file_path = os.path.join(temp_dir, file)
                            os.remove(file_path)
                            print(f"Eliminado: {file_path}")
                    else:
                        file_path = os.path.join(temp_dir, file)
                        os.remove(file_path)
                        print(f"Eliminado: {file_path}")
        except Exception as e:
            print(f"Error al limpiar caché: {e}")
            
    def s3_presigned_url(self, key, expires= 3600):
        try:
            if not hasattr(self, 's3_client') or not hasattr(self, 'bucket'):
                raise AttributeError("s3_client o bucket no definidos en el objeto.")

            if not key:
                raise ValueError("El parámetro 'key' está vacío o es inválido.")

            presigned_url = self.s3_client.generate_presigned_url(
                'get_object',
                Params={'Bucket': self.bucket, 'Key': key},
                ExpiresIn=expires
            )
            return presigned_url if presigned_url else ''
        except Exception as e:
            print(f"[WARN] No se pudo generar la URL prefirmada: {str(e)}")
            return ''

    def download_directory(self, s3_folder: str, local_dir: str | None = None) -> None:
        """
        Descarga todo el contenido de un prefijo de S3 en un directorio local.
        """
        if local_dir is None:
            local_dir = os.getcwd()

        try:
            paginator = self.s3_client.get_paginator("list_objects_v2")
            for result in paginator.paginate(Bucket=self.bucket, Prefix=s3_folder):
                if result.get("Contents"):
                    for file in result["Contents"]:
                        file_key = file["Key"]
                        # quitar el prefijo de la carpeta
                        if file_key.startswith(s3_folder):
                            rel_path = file_key[len(s3_folder):].lstrip("/")
                            local_file_path = os.path.join(local_dir, rel_path)

                            os.makedirs(os.path.dirname(local_file_path), exist_ok=True)
                            self.s3_client.download_file(self.bucket, file_key, local_file_path)
                            print(f"Downloaded {file_key} → {local_file_path}")
        except Exception as e:
            print(f"[WARN] No se pudo descargar el directorio: {str(e)}")
            return ''
                    


    def upload_directory_to_s3(self, local_directory: str, s3_folder: str) -> None:
        """
        Sube recursivamente un directorio local a S3 bajo el prefijo s3_folder.
        """
        for root, _, files in os.walk(local_directory):
            for file in files:
                local_path = os.path.join(root, file)
                relative_path = os.path.relpath(local_path, local_directory)
                s3_path = os.path.join(s3_folder, relative_path).replace("\\", "/")

                try:
                    self.s3_client.upload_file(local_path, self.bucket, s3_path)
                    print(f"Upload Successful: {s3_path}")
                except FileNotFoundError:
                    print(f"File not found: {local_path}")
                except NoCredentialsError:
                    print("Credentials not available")

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
    
    def create_table(self, table_name, partition_key):
        """Crea una nueva tabla en DynamoDB si no existe."""
        try:
            table = self.dynamodb.create_table(
                TableName=table_name,
                KeySchema=[{'AttributeName': partition_key, 'KeyType': 'HASH'}],  # Clave primaria
                AttributeDefinitions=[{'AttributeName': partition_key, 'AttributeType': 'S'}],
                ProvisionedThroughput={'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
            )
            table.wait_until_exists()
            logging.info(f"Table {table_name} created successfully.")
            return table
        except ClientError as e:
            logging.error(f"Error creating table {table_name}: {str(e)}")
            return None
        
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
