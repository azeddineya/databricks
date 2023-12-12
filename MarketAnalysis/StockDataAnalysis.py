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
# COMMAND ----------
df.select("Date", "Adj Close", "Volume") \
    .where(df.Volume > 150000000) \
    .where.option("header","true") \
        .mode("overwrite")\
        .csv("dbfs:/user/hive/warehouse/output/tesla_highvoldays")
