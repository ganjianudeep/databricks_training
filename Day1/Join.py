# Databricks notebook source
df_sales=spark.table("sales")

# COMMAND ----------

df_customer=spark.table("customers")

# COMMAND ----------

df_joined=df_sales.join(df_customer,df_sales["customer_id"]==df_customer["customer_id"],"inner")

# COMMAND ----------

df_joined.display()

# COMMAND ----------

df_customer.display()

# COMMAND ----------

df_customer.filter("customer_id=2").display()

# from pyspark.sql.functions import *
# df_customers.where(col("customer_id")==2).display()

# COMMAND ----------

df_customer.sort("customer_city").display()

#df_customer.sort("customer_city",ascending=False).display() -> for ascending
#df_customer.sort(col("customer_city").desc()).display()

# COMMAND ----------

df_joined.groupBy("sales.customer_id").count().orderBy("sales.customer_id").display()

# COMMAND ----------

# MAGIC %sql
# MAGIC select customer_id from customers group by customer_id
