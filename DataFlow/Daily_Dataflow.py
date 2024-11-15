import os
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions,GoogleCloudOptions
import logging
import requests
from apache_beam.io.parquetio import WriteToParquet,ReadFromParquet
from google.cloud import storage
import json
from pyspark.sql.types import StructType,StringType,DecimalType,LongType,FloatType,IntegerType,StructField,DoubleType
from pyspark.sql import SparkSession
from datetime import *
import pyarrow as pa
from apache_beam.io import parquetio

logging.getLogger().setLevel(logging.INFO)

if __name__=='__main__':

    os.environ['GOOGLE_APPLICATION_CREDENTIALS']=r'C:\Users\Lenovo\Earthquake\my-project-2024-441519-a619121f3799.json'

    options=PipelineOptions()
    #To make the connection with the cloud platform
    google_cloud_options=options.view_as(GoogleCloudOptions)
    google_cloud_options.project='my-project-2024-441519'
    google_cloud_options.job_name='historical-data'
    google_cloud_options.region='us-central1'
    google_cloud_options.staging_location='gs://earthquake_analysissd/Locations/Stage/'
    google_cloud_options.temp_location = 'gs://earthquake_analysissd/Locations/temp/'

    current_date=datetime.now().strftime('%Y%m%d')
    daily_data_url = 'https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson'
    bucket_name="earthquake_analysissd"
    destination_blob_name=f'Dataflow/landing/{current_date}/Daily_data.json'
    gcs_path=f"gs://{bucket_name}/{destination_blob_name}"
    project_id='my-project-2024-441519'
    dataset='earthquake_db'

    bq_schema = {
            "fields": [
            {"name": "mag", "type": "FLOAT"},
            {"name": "place", "type": "STRING"},
            {"name": "time", "type": "DATETIME"},
            {"name": "updated", "type": "DATETIME"},
            {"name": "tz", "type": "INTEGER"},
            {"name": "url", "type": "STRING"},
            {"name": "detail", "type": "STRING"},
            {"name": "felt", "type": "INTEGER"},
            {"name": "cdi", "type": "FLOAT"},
            {"name": "mmi", "type": "FLOAT"},
            {"name": "alert", "type": "STRING"},
            {"name": "status", "type": "STRING"},
            {"name": "tsunami", "type": "INTEGER"},
            {"name": "sig", "type": "INTEGER"},
            {"name": "net", "type": "STRING"},
            {"name": "code", "type": "STRING"},
            {"name": "ids", "type": "STRING"},
            {"name": "sources", "type": "STRING"},
            {"name": "types", "type": "STRING"},
            {"name": "nst", "type": "INTEGER"},
            {"name": "dmin", "type": "FLOAT"},
            {"name": "rms", "type": "FLOAT"},
            {"name": "gap", "type": "FLOAT"},
            {"name": "magType", "type": "STRING"},
            {"name": "type", "type": "STRING"},
            {"name": "title", "type": "STRING"},
            {"name": "longitude", "type": "FLOAT"},
            {"name": "latitude", "type": "FLOAT"},
            {"name": "depth", "type": "FLOAT"},
            {"name": "area", "type": "STRING"},
            {"name": "timestamp", "type": "DATETIME"}
            ]}
    def parquet_schema():
        return pa.schema([
            ("mag", pa.float64()),
            ("place", pa.string()),
            ("time", pa.string()),  # Keep as string if already formatted
            ("updated", pa.string()),  # Keep as string if already formatted
            ("tz", pa.int64()),
            ("url", pa.string()),
            ("detail", pa.string()),
            ("felt", pa.int64()),
            ("cdi", pa.float64()),
            ("mmi", pa.float64()),
            ("alert", pa.string()),
            ("status", pa.string()),
            ("tsunami", pa.int64()),
            ("sig", pa.int64()),
            ("net", pa.string()),
            ("code", pa.string()),
            ("ids", pa.string()),
            ("sources", pa.string()),
            ("types", pa.string()),
            ("nst", pa.int64()),
            ("dmin", pa.float64()),
            ("rms", pa.float64()),
            ("gap", pa.float64()),
            ("magType", pa.string()),
            ("type", pa.string()),
            ("title", pa.string()),
            ("longitude", pa.float64()),
            ("latitude", pa.float64()),
            ("depth", pa.float64()),
            ("area", pa.string())
        ])
    def read_data(url):
        try:
            response=requests.get(url)
            logging.info('URL data has been successfully read!')
            return response.json()
        except Exception as msg:
            logging.error(f'Error while reading URL: {msg}')
            return None
    def upload_to_gcs(bucket_name, url_data, destination_blob_name):
        try:
            client = storage.Client()

            bucket = client.bucket(bucket_name)
            blob = bucket.blob(destination_blob_name)
            blob.upload_from_string(json.dumps(url_data), content_type='application/json')
            print('Data successfully uploaded into GCS ')
        except Exception as msg:
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

    def time_conversion(timet, timeup):
        """
        This function help us to conversion of EPOCH to Local timestamp

        :param timet:covert the time column epoch to timestamp
        :param timeup:covert the updated time column epoch to timestamp
        :return: return timestamp
        """
        time1 = datetime.utcfromtimestamp(timet / 1000).strftime('%Y-%m-%d  %H:%M:%S')
        updated1 = datetime.utcfromtimestamp(timeup / 1000).strftime('%Y-%m-%d  %H:%M:%S')
        return time1, updated1
    def add_region(area):
        if " of " in area:
            return area.split(" of ", 1)[1]
        else:
            return area
    class flattened_data(beam.DoFn):
        def process(self, even_data):

            event_data = json.loads(even_data)
            features = event_data.get('features', [])

            for feature in features:
                properties = feature["properties"]
                geometry = feature["geometry"]
                coordinates = geometry["coordinates"]

                # Columns like “time”, “updated”- convert its value from epoch to timestamp
                # Generate column “area”- based on existing “place” colum
                place = properties.get("place")
                area = add_region(place)

                times = properties.get("time")
                timeup1 = properties.get("updated")

                time1, updated1 = time_conversion(times, timeup1)

                flat_event = {
                    "mag": float(properties.get("mag")) if properties.get("mag") is not None else None,
                    "place": place,
                    "time": time1,
                    "updated": updated1,
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
                    "depth": float(coordinates[2]) if coordinates[2] is not None else None,
                    "area":area
                }

                yield flat_event
    def insert_date(element):
        element['timestamp'] = datetime.now().strftime('%Y-%m-%d  %H:%M:%S')
        return element

    #Read data from the URL
    with beam.Pipeline(options=options) as p:
        res =(p | "Start" >> beam.Create([daily_data_url])
                | 'Read data from the URL'>>beam.Map(read_data)
                | "Upload data to GCS">>beam.ParDo(lambda x: upload_to_gcs(bucket_name=bucket_name,
                                                                            url_data=x,
                                                                            destination_blob_name=f'Dataflow/landing/{current_date}/Daily_data.json'))
                 # | "print">>beam.Map(lambda _:print(f"Data successfully uploaded to GCS"))
               )

    #Write to the parquet file
    with beam.Pipeline(options=options) as p1:
        res1=(p1 | "Read data from GCS" >> beam.io.ReadFromText(gcs_path)
                 | "Flatten data" >> beam.ParDo( flattened_data())
                 | "Write data to GCS as Parquet" >>beam.io.WriteToParquet(file_path_prefix=f'gs://{bucket_name}/Silver/{current_date}/Daily_dataflow',
                                                                           schema=parquet_schema(),
                                                                           file_name_suffix=".parquet",
                                                                           codec="snappy"))
    #Read the data from the GCS and load to bigquery
    with beam.Pipeline(options=options) as p3:
        res3=(p3| "read data from the GCS">>beam.io.ReadFromParquet(file_pattern=f'gs://{bucket_name}/Silver/{current_date}/Daily_dataflow-*.parquet')
                | 'Insert timestamp col'>>beam.Map(insert_date)
                # | 'print'>>beam.Map(print))
                | 'upload to bigquery'>>beam.io.WriteToBigQuery(table=f'{project_id}.{dataset}.earthquake_dataflow',
                                                                schema=bq_schema,
                                                                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                                                                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                                custom_gcs_temp_location='gs://earthquake_analysissd/Locations/temp/' ))

















