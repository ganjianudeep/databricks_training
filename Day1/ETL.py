# Databricks notebook source
# MAGIC %run /Workspace/Users/anudeep2k@gmail.com/Day1/includes

# COMMAND ----------

df_sales=spark.read.csv(f"{input_path}sales.csv",header=True,inferSchema=True)

# COMMAND ----------

df1=add_ingestion(df_sales)

# COMMAND ----------

df1.write.mode("overwrite").saveAsTable("order_dates")

# COMMAND ----------


