# COMMAND ----------
from pyspark.sql.functions import year, month, sum, col

df_silver = spark.table("superstore_catalog.silver.orders_cleaned")

df_category = df_silver.groupBy("category", "sub_category") \
    .agg(
        sum("sales").alias("total_sales"),
        sum("profit").alias("total_profit")
    )

df_category.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("superstore_catalog.gold.category_performance")

# COMMAND ----------
df_loss = df_silver.filter(col("profit") < 0)

df_loss.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("superstore_catalog.gold.loss_products")

# COMMAND ----------

df_region = df_silver.groupBy("region") \
    .agg(
        sum("sales").alias("total_sales"),
        sum("profit").alias("total_profit")
    )

df_region.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("superstore_catalog.gold.region_performance")