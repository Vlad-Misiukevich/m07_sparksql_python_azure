# Databricks notebook source
# MAGIC %sql
# MAGIC Create table second as
# MAGIC select 
# MAGIC name,
# MAGIC address,
# MAGIC avg_temp_for_stay,
# MAGIC last_wthr_date_temp - first_wthr_date_temp as wthr_trend 
# MAGIC from (
# MAGIC select
# MAGIC   hotel_id,
# MAGIC   name,
# MAGIC   address,
# MAGIC   avg(avg_tmpr_c) as avg_temp_for_stay,
# MAGIC   first(avg_tmpr_c, true) as first_wthr_date_temp,
# MAGIC   last(avg_tmpr_c, true)as last_wthr_date_temp
# MAGIC   from
# MAGIC (select 
# MAGIC   *
# MAGIC from
# MAGIC (select
# MAGIC   hotel_id,
# MAGIC   srch_ci,
# MAGIC   srch_co,
# MAGIC   datediff(srch_co, srch_ci) as n_days_stay
# MAGIC   from expedia_delta where datediff(srch_co, srch_ci) > 7) as t
# MAGIC   left join hotel_weather_delta on id = t.hotel_id and wthr_date between t.srch_ci and t.srch_co
# MAGIC     sort by wthr_date) as tt
# MAGIC   group by hotel_id, name, address) where name is not null

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
df = spark.table("default.second")
df.write.format("parquet").save("abfss://data@stvmisiukevich.dfs.core.windows.net/spark_hw_2/result/weather_trends")
