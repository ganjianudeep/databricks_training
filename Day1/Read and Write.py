# Databricks notebook source
df=spark.read.csv("/Volumes/my_databricks_ws/default/raw/sales.csv",header=True,inferSchema=True)

# COMMAND ----------

df.display()

# COMMAND ----------

df.write.mode("overwrite").saveAsTable("sales")

# COMMAND ----------

df_customers=spark.read.json(
"/Volumes/my_databricks_ws/default/raw/customers.json"
)

# COMMAND ----------

df_customers.write.saveAsTable("customer")  
