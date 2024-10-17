# Databricks notebook source
print("hello world")

# COMMAND ----------

# MAGIC %md
# MAGIC RDD dataframe

# COMMAND ----------

data = [(1,'a',10),(2,'b',20)]
schema=["id","name","age"]
df=spark.createDataFrame(data,schema)

# COMMAND ----------

display(df)

# COMMAND ----------

# schema=["id.int","name.string","age.int"] -> for dataypes

# COMMAND ----------

# DataFrme functions
df.select("*").display()

# COMMAND ----------

df.select("id".alias("emp-id"))

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df.select(col("id").alias("emp-id")).display()

# COMMAND ----------

df.withColumnRenamed("id","emp_id").display()

# COMMAND ----------

df.withColumn("currrent_date",current_date()).display()
