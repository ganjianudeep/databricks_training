# Databricks notebook source
# MAGIC %sql
# MAGIC select * from file_format.`path`

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE table customers as
# MAGIC select *,current_timestamp() as ingestion_date from json.`/Volumes/my_databricks_ws/default/raw/customers.json`

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE table products as
# MAGIC select *,current_timestamp() as ingestion_date from json.`/Volumes/my_databricks_ws/default/raw/products.json`

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table customer
