# COMMAND ----------

df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("file:////Workspace/SuperStore Sales/orders.csv")

df.display()

# COMMAND ----------

from pyspark.sql.functions import to_date

df_validated = df.withColumn(
    "order_date_valid",
    to_date("order_date", "MM/dd/yyyy")
)

df_validated.select("order_date", "order_date_valid").display()

# COMMAND ----------

from pyspark.sql.functions import col

df_numeric_check = df.filter(
    col("sales").isNull() | 
    col("profit").isNull() | 
    col("quantity").isNull()
)
df_numeric_check.display()

# COMMAND ----------

df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("superstore_catalog.bronze.orders_raw")


# COMMAND ----------

df_bronze = spark.table("superstore_catalog.bronze.orders_raw")

df_clean = df_bronze.dropDuplicates()