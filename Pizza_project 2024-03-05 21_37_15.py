# Databricks notebook source
# DBTITLE 1,Taking data source from blob storage to databrics(dbfs:/mnt/raw3/dbo.pizza_sales.txt)
dbutils.fs.mount(
    source= "wasbs://raw@guptdhan007.blob.core.windows.net",
    mount_point = "/mnt/raw3",
    extra_configs = {"fs.azure.account.key.guptdhan007.blob.core.windows.net": "N/DceQ2cBOd+93U8hB1ZLA89KT1lskC6nS7K72ETc5uIrDWeJ7+h0hLbATOozuGSl3gGPxYhsDiM+AStkNiPxg=="}
)

# COMMAND ----------

dbutils.fs.ls("/mnt/raw3")

# COMMAND ----------

df = spark.read.format("csv").options(header="True",inferSchema='True').load("dbfs:/mnt/raw3/dbo.pizza_sales.txt")

# COMMAND ----------

display(df)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.show(2)

# COMMAND ----------

df.createOrReplaceTempView("pizza_sales_analysis")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from  pizza_sales_analysis
# MAGIC limit 3

# COMMAND ----------

# DBTITLE 1,Select required column
# MAGIC %sql
# MAGIC select 
# MAGIC count(distinct order_id) order_id,
# MAGIC sum(quantity) quantity,
# MAGIC date_format(order_date,'MMM') month_name,
# MAGIC date_format(order_date,'EEEE') day_name,
# MAGIC hour(order_time) order_time,
# MAGIC sum(unit_price) unit_price,
# MAGIC sum(total_price) total_price,
# MAGIC pizza_size,
# MAGIC pizza_category,
# MAGIC pizza_name
# MAGIC from  pizza_sales_analysis
# MAGIC group by 3,4,5,8,9,10

# COMMAND ----------


