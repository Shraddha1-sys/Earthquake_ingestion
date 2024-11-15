#################################################################################
# Name : Load Historical Data By Pyspark.py
# Description : Load the data after flattening
# Date : 2024 - 10 - 22
# Version : 3.3.2
# Modified date : 2024- 11 - 10
# Modification Description :
##################################################################################
import util
import json
import os
from datetime import *
import requests
from pyspark.sql import SparkSession
from google.cloud import storage
from pyspark.sql.functions import SparkContext,StructType,col,split,lit,unix_timestamp,from_unixtime,current_timestamp
from pyspark.sql.types import StructType,StringType,DecimalType,LongType,FloatType,IntegerType,StructField,DoubleType

if __name__=='__main__':

    spark = SparkSession.builder.appName('earthquake').getOrCreate()

    # os.environ['GOOGLE_APPLICATION_CREDENTIALS']=r'C:\Users\Lenovo\Earthquake\my-project-2024-441519-a619121f3799.json'

    #read the data from the URL ::--
    url='https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.geojson'

    #======================================================================
    #read data from the url
    data=util.read_data_from_url(url)

    # ======================================================================
    #upload to gcs
    current_date=datetime.now().strftime('%Y%m%d')
    upload_data=util.upload_data_to_gcs(bucket_name='earthquake_analysissd',url_data=data,destination_blob_name=f'pyspark/landing/{current_date}/Historical_data.json')

    # ======================================================================
    # Read the data from the GCS
    read_data=util.read_from_gcs('earthquake_analysissd',f'pyspark/landing/{current_date}/Historical_data.json')
    print('data read from GCS')

    #======================================================================
    #for creating dataframe we need to create the schema and the list of elements
    schema=util.schema
    data=util.flatten_earthquake_data(read_data)

    df =spark.createDataFrame(data,schema=schema)
    # df.show()

    # ===========================TRANSFORMATION================================

    #Columns like “time”, “updated”- convert its value from epoch to timestamp
    dff=(df.withColumn('time',from_unixtime(col('time') / 1000))
         .withColumn('updated', from_unixtime(col('updated') / 1000))
         .withColumn('area',split(col("place"), " of").getItem(1)))

    # # ======================================================================
    # Insert data : insert_dt (Timestamp
    df1=dff.withColumn('insert_dt',lit(current_timestamp()))

    gcs_path = f"gs://earthquake_analysissd/Silver/{current_date}/Historical_data.parquet"
    df1.write.mode("overwrite").parquet(gcs_path)
    print('Data successfully load to the silver layer')

    # Load data into Bigquery
    df1.write \
    .format('bigquery') \
    .option('table','my-project-2024-441519.earthquake_db.earthquake_data') \
    .option('parentProject', 'my-project-2024-441519') \
    .option("temporaryGcsBucket", "earthquake_analysissd") \
    .mode('overwrite') \
    .save()

    print('Loaded data successfully into Bigquery..!!')

























