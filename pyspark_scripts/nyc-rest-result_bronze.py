#!/usr/bin/env python
# coding: utf-8

# In[9]:
import pyspark
from pyspark.sql import SparkSession

# In[10]:
spark = SparkSession.builder.appName("write to GCS").getOrCreate()

# In[14]:
input_path="gs://all-row-datasets-files/resturent_nyc_dataset/NYC_Restaurant_Inspection_Results_20250121.csv"
df = spark.read.csv(input_path,inferSchema=True)

# In[21]:
df.printSchema()
df.count()

# In[33]:
bucket_name = "bronze-output-datasets"
file_path = f"gs://{bucket_name}/NYC Restaurant Inspection/bronze_data"
df.write.mode("overwrite").csv(file_path)

spark.stop()
