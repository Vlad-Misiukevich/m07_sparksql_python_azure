# Databricks notebook source
# MAGIC %sql
# MAGIC Create table first as
# MAGIC select * from (
# MAGIC select 
# MAGIC   id,
# MAGIC   max(temp_delta_abs) as max_temp_delta_abs
# MAGIC from (
# MAGIC   select
# MAGIC     id,
# MAGIC     year,
# MAGIC     month,
# MAGIC     abs(min(avg_tmpr_c) - max(avg_tmpr_c)) as temp_delta_abs 
# MAGIC   from hotel_weather_delta group by id, year, month
# MAGIC     ) group by id) as t
# MAGIC     join (select distinct id, name, address from hotel_weather_delta) using (id)
# MAGIC     sort by max_temp_delta_abs desc limit 10;

# COMMAND ----------

from pyspark.conf import SparkConf
from pyspark.sql import SparkSession

app_name = "DataExtract"
master = "local[*]"
spark_conf = SparkConf() \
    .setAppName(app_name) \
    .setMaster(master) \
    .set("fs.azure.account.auth.type.bd201stacc.dfs.core.windows.net", "OAuth") \
    .set("fs.azure.account.oauth.provider.type.bd201stacc.dfs.core.windows.net",
         "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider") \
    .set("fs.azure.account.oauth2.client.secret.bd201stacc.dfs.core.windows.net",
         "mAwIU~M4~xMYHi4YX_uT8qQ.ta2.LTYZxT") \
    .set("fs.azure.account.oauth2.client.endpoint.bd201stacc.dfs.core.windows.net",
         "https://login.microsoftonline.com/b41b72d0-4e9f-4c26-8a69-f949f367c91d/oauth2/token") \
    .set("fs.azure.account.oauth2.client.id.bd201stacc.dfs.core.windows.net",
         "f3905ff9-16d4-43ac-9011-842b661d556d")
spark_obj = SparkSession.builder.config(conf=spark_conf).getOrCreate()

spark_obj.conf.set(
    "fs.azure.account.key.stvmisiukevich.dfs.core.windows.net",
    "CrJFosuX/xvxRRXYJQiCM8WlDsc/+Cy0Whe0DWN7uuPoKCTx+WeEL9XyX72WfwQB51v+D/qJ6STqA9d5ej7UZw=="
)
df = spark.table("default.first")
df.write.format("parquet").save("abfss://data@stvmisiukevich.dfs.core.windows.net/spark_hw_2/result/top_ten_hotels_with_max_tempr_diff_by_month")
