# Databricks notebook source
# MAGIC %sql
# MAGIC Create table IF NOT EXISTS second as
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

spark.conf.set(
    "fs.azure.account.key.stvmisiukevich.dfs.core.windows.net",
    "CrJFosuX/xvxRRXYJQiCM8WlDsc/+Cy0Whe0DWN7uuPoKCTx+WeEL9XyX72WfwQB51v+D/qJ6STqA9d5ej7UZw=="
)
df = spark.table("default.second")
df.write.format("parquet").save("abfss://data@stvmisiukevich.dfs.core.windows.net/weather")

# COMMAND ----------

# MAGIC %sql
# MAGIC Create table IF NOT EXISTS first as
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

df = spark.table("default.first")
df.write.format("parquet").save("abfss://data@stvmisiukevich.dfs.core.windows.net/top_ten_hotels_with_max_tempr_diff_by_month")

# COMMAND ----------

from pyspark.sql import functions as F


expedia_delta = spark.read.table("expedia_delta")

expedia_tmp = (
    expedia_delta
    .selectExpr("id","hotel_id","month(srch_ci) AS month_range")
)

expedia_tmp = (expedia_tmp
    .select("id", "hotel_id", "month_range").distinct())

expedia_tmp = (
    expedia_tmp
        .groupBy("hotel_id", "month_range").agg(F.count("*").alias('visits')).orderBy(F.col('visits').desc()).limit(10)
)

hotel_weather_df = spark.read.table("hotel_weather_delta")

top_ten_busy_hotels_df = (
    expedia_tmp
    .join(
        hotel_weather_df.select("id", "name").distinct(),
        on=[
            expedia_tmp.hotel_id == hotel_weather_df.id],
        how="inner").orderBy(F.col('visits').desc()).drop("id"))

top_ten_busy_hotels_df.write.format("parquet").save("abfss://data@stvmisiukevich.dfs.core.windows.net/top_ten_busy_hotels")
