# Databricks notebook source
from pyspark.sql.functions import max

# COMMAND ----------

df = spark.read.format("delta").load("dbfs:/user/hive/warehouse/tesla_data_2020")
display(df)

# COMMAND ----------

df.select(max("Adj Close"), max("Volume")) \
    .withColumnRenamed("max(Adj Close)","Max Close") \
        .withColumnRenamed("max(Volume)", "Max Volume") \
            .show(truncate=False)
