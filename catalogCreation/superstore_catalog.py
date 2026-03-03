%sql
-- Create Catalog
CREATE CATALOG IF NOT EXISTS superstore_catalog
MANAGED LOCATION 'abfss://unity-catalog-storage@dbstorageh5zdxpvtstqcc.dfs.core.windows.net/7405614075489593';

-- Create Schemas (Medallion Layers)
CREATE SCHEMA IF NOT EXISTS superstore_catalog.superstore_schema;

%sql
USE CATALOG superstore_catalog;

CREATE SCHEMA bronze
COMMENT 'Raw Ingested Data Layer';

CREATE SCHEMA silver
COMMENT 'Cleaned and Transformed Data Layer';

CREATE SCHEMA gold
COMMENT 'Business Aggregated Data Layer';

spark.sql("SHOW SCHEMAS IN superstore_catalog")

df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("file:////Workspace/SuperStore Sales/orders.csv")

display(df)
df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("superstore_catalog.bronze.orders_raw")
