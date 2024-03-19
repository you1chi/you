# Databricks notebook source
# MAGIC %md
# MAGIC # WORKSHOP

# COMMAND ----------

from pyspark.sql.functions import datediff, current_date
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import avg

# COMMAND ----------

df_laptimes = spark.read.csv('s3://columbia-gr5069-main/raw/lap_times.csv', header = True)

# COMMAND ----------

display(df_laptimes)

# COMMAND ----------

df_driver = spark.read.csv('s3://columbia-gr5069-main/raw/drivers.csv', header = True)
df_driver.count()

# COMMAND ----------

display(df_driver)

# COMMAND ----------

df_driver = df_driver.withColumn('age',datediff(current_date(),df_driver.dob)/365)

# COMMAND ----------

display(df_driver)

# COMMAND ----------

df_driver = df_driver.withColumn('age',df_driver['age'].cast(IntegerType()))
display(df_driver)

# COMMAND ----------

df_lap_drivers = df_driver.select('nationality','age','forename','surname','url','driverId').join(df_laptimes, on=['driverId'])
display(df_lap_drivers)

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Aggregate by Age

# COMMAND ----------

df_lap_drivers = df_lap_drivers.groupby('nationality','age').agg(avg('time'))
display(df_lap_drivers)

# COMMAND ----------

df_lap_drivers.write.csv('s3://columnbia-gr5069/you.csv')
