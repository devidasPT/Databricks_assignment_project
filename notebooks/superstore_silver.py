# COMMAND ----------
from pyspark.sql.functions import col, to_date, year, month, sum, lower, trim

df_bronze = spark.table("superstore_catalog.bronze.orders_raw")
df_clean = df_bronze.dropDuplicates()

# COMMAND ----------
df_bronze = spark.table("superstore_catalog.bronze.orders_raw")

df_clean = df_bronze.dropDuplicates()

df_clean = df_clean.fillna({
    "discount": 0,
    "profit": 0
})

# COMMAND ----------
from pyspark.sql.functions import lower, trim, col

df_clean = df_clean \
    .withColumn("region", lower(trim(col("region")))) \
    .withColumn("category", lower(trim(col("category")))) \
    .withColumn("sub_category", lower(trim(col("sub_category"))))


# COMMAND ----------
df_clean = df_clean.withColumn(
    "order_date",
    to_date("order_date", "MM/dd/yyyy")
)
df_clean = df_clean.withColumn(
    "profit_margin",
    (col("profit") / col("sales")) * 100
)
df_clean = df_clean.withColumn(
    "total_sales",
    col("sales") * col("quantity")
)

# COMMAND ----------
df_clean.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("superstore_catalog.silver.orders_cleaned")

# COMMAND ----------
from pyspark.sql.functions import year, month, sum

df_silver = spark.table("superstore_catalog.silver.orders_cleaned")

df_trend = df_silver \
    .withColumn("year", year("order_date")) \
    .withColumn("month", month("order_date")) \
    .groupBy("year", "month") \
    .agg(
        sum("sales").alias("total_sales"),
        sum("profit").alias("total_profit")
    )

df_trend.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("superstore_catalog.gold.sales_trend")