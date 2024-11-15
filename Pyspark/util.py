import json
import os
from datetime import *
import requests
from pyspark.sql import SparkSession
from google.cloud import storage
from pyspark.sql.functions import SparkContext,StructType
from pyspark.sql.types import StructType,StringType,DecimalType,LongType,FloatType,IntegerType,StructField,DoubleType


def read_data_from_url(url):
    try:
        response = requests.get(url)
        print('Data read from the URL')
        return response.json()
    except Exception as e:
        print(f'We can not read the data from the url ,ERROR is {e}')

def upload_data_to_gcs(bucket_name, url_data, destination_blob_name):
    try:
        client = storage.Client()

        bucket = client.bucket(bucket_name)
        blob =bucket.blob(destination_blob_name)
        blob.upload_from_string(json.dumps(url_data), content_type='application/json')
        print('Data successfully uploaded into GCS at bronze level')
    except Exception as msg :
        print(f'We are facing error while uploading ::- ERROR Occupied : {msg}')

def read_from_gcs(bucket_name, blob_name):
    try:
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        content = blob.download_as_text()
        data = json.loads(content)
        return data
    except Exception as e:
        print(f'We can not read the data from the GCS ,ERROR is {e}')

schema = StructType([
        StructField("mag", DoubleType(), True),
        StructField("place", StringType(), True),
        StructField("time", LongType(), True),
        StructField("updated", LongType(), True),
        StructField("tz", IntegerType(), True),
        StructField("url", StringType(), True),
        StructField("detail", StringType(), True),
        StructField("felt", IntegerType(), True),
        StructField("cdi", FloatType(), True),
        StructField("mmi", FloatType(), True),
        StructField("alert", StringType(), True),
        StructField("status", StringType(), True),
        StructField("tsunami", IntegerType(), True),
        StructField("sig", IntegerType(), True),
        StructField("net", StringType(), True),
        StructField("code", StringType(), True),
        StructField("ids", StringType(), True),
        StructField("sources", StringType(), True),
        StructField("types", StringType(), True),
        StructField("nst", IntegerType(), True),
        StructField("dmin", FloatType(), True),
        StructField("rms", FloatType(), True),
        StructField("gap", FloatType(), True),
        StructField("magType", StringType(), True),
        StructField("type", StringType(), True),
        StructField("title", StringType(), True),
        StructField("longitude", FloatType(), True),
        StructField("latitude", FloatType(), True),
        StructField("depth", FloatType(), True)
    ])

def flatten_earthquake_data(even_data):
    """
    :param even_data: data which needs to be flattened
    :return: return flattened data into list of dict
    """

    flattened_data = []
    features = even_data.get('features', [])

    for feature in features:
        properties = feature["properties"]
        geometry = feature["geometry"]
        coordinates = geometry["coordinates"]

        flat_event = {
            "mag": float(properties.get("mag")) if properties.get("mag") is not None else None,
            "place": properties.get("place"),
            "time": properties.get("time"),
            "updated": properties.get("updated"),
            "tz": properties.get("tz"),
            "url": properties.get("url"),
            "detail": properties.get("detail"),
            "felt": properties.get("felt"),
            "cdi": float(properties.get("cdi")) if properties.get("cdi") is not None else None,
            "mmi": float(properties.get("mmi")) if properties.get("mmi") is not None else None,
            "alert": properties.get("alert"),
            "status": properties.get("status"),
            "tsunami": properties.get("tsunami"),
            "sig": properties.get("sig"),
            "net": properties.get("net"),
            "code": properties.get("code"),
            "ids": properties.get("ids"),
            "sources": properties.get("sources"),
            "types": properties.get("types"),
            "nst": properties.get("nst"),
            "dmin": float(properties.get("dmin")) if properties.get("dmin") is not None else None,
            "rms": float(properties.get("rms")) if properties.get("rms") is not None else None,
            "gap": float(properties.get("gap")) if properties.get("gap") is not None else None,
            "magType": properties.get("magType"),
            "type": properties.get("type"),
            "title": properties.get("title"),
            "longitude": coordinates[0],
            "latitude": coordinates[1],
            "depth": float(coordinates[2]) if coordinates[2] is not None else None
        }

        flattened_data.append(flat_event)
    return flattened_data
