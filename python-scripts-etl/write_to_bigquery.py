#!/usr/bin/env python
# coding: utf-8

# In[1]:
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# In[2]:
spark = SparkSession.builder.appName("Write to BigQuery").getOrCreate()

# In[3]:
input_path="gs://all-row-datasets-files/resturent_nyc_dataset/NYC_Restaurant_Inspection_Results_20250121.csv"
df = spark.read.csv(input_path,header=True,inferSchema=True)

# In[4]:
dropdup = df.dropDuplicates()
dropdup.count()

# In[5]:
dropna = dropdup.dropna(subset="CAMIS")
dropna.count()

# In[6]:
df_cleaned = dropna.select(
    [F.col(column).alias(column.replace(" ", "_").replace(":", "_")) for column in dropna.columns]
)
print("Cleaned Column Names:", df_cleaned.columns)

# In[7]:
df_uppercase = df_cleaned.toDF(*[col.upper() for col in df.columns])

# In[8]:
print(df_uppercase)

# In[9]:
df_cleaned.write.format("bigquery").option("table","dotted-banner-448417-n1.nyc_rest_results.rest_results_nyc").option("temporaryGcsBucket", "resturent_nyc_dataset").mode("overwrite").save()

# In[10]:
spark.stop()
