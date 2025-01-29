#!/usr/bin/env python
# coding: utf-8

# In[1]:
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,IntegerType,StringType
from pyspark.sql.functions import trim, col, when, lower

# In[2]:
spark = SparkSession.builder.appName("players_ranking").getOrCreate()

# In[3]:
schema = StructType([     StructField("id",IntegerType(),True),     StructField("rank",IntegerType(),True),     StructField("name",StringType(),True),     StructField("country", StringType(), True),     StructField("rating", IntegerType(), True),     StructField("points", IntegerType(), True),     StructField("trend", StringType(), True),     StructField("faceImageId", IntegerType(), True)     ])

# In[4]:
input_path = "gs://all-row-datasets-files/crickbuzz_ranking/batsmen_rankings.csv"
read_df = spark.read.csv(input_path,schema=schema)

# In[5]:
drop_df = read_df.drop("faceImageId")

# In[6]:
transform_df = drop_df.withColumn("CATEGORY", when(drop_df.rating >= 850,"Elite").when((drop_df.rating >=700) & (drop_df.rating <850),"Top Performer").otherwise("Good"))

# In[7]:
df_uppercase = transform_df.toDF(*[col.upper() for col in transform_df.columns])

# In[10]:
df_transformed = df_uppercase.withColumn("TREND", lower(df_uppercase["TREND"]))

# In[11]:
output_path = "gs://bronze-output-datasets/crickbuzz_ranking"
df_transformed.coalesce(1).write.mode("overwrite").parquet(output_path)

# In[12]:
spark.stop()