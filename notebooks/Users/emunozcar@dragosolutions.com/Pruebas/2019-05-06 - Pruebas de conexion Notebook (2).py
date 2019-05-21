# Databricks notebook source
storage_account_name = "Nombre del storage"
storage_account_access_key = "clave de acceso al storage"

# COMMAND ----------

file_location = "wasbs://practica001/Multas/"
file_type = "csv"

# COMMAND ----------

spark.conf.set(
  "fs.azure.account.key."+storage_account_name+".blob.core.windows.net",
  storage_account_access_key)


# COMMAND ----------

df = spark.read.format("csv").option("header", "true").option("delimiter", ";").load("wasbs://practica001@strge0001.blob.core.windows.net/Multas")

dbutils.fs.ls("wasbs://practica001@strge0001.blob.core.windows.net/Multas")



# COMMAND ----------

# ESTA ES UNA PRUEBA
display(df)

# COMMAND ----------

#Branch Ana adicion 01

# COMMAND ----------

#branch Ana Adicion 02