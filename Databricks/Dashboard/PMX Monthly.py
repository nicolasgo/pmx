# Databricks notebook source exported at Thu, 23 Apr 2015 18:33:33 UTC
rdds = sc.textFile("/pv/10/201503")

# COMMAND ----------

rows = rdds.collect()

# COMMAND ----------

len(rows)
for row in rows[0:5]:
  print row

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/pv/10/201503"))
