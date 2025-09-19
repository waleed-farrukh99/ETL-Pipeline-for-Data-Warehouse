from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name
import os


spark = SparkSession.builder.getOrCreate()


# paths
landing_path = '/mnt/dw_raw/landing/sales_files'
bronze_path = '/mnt/dw_bronze/sales'


# ingest: read CSVs and write as Delta to bronze
df = (spark.read
.option('header', True)
.option('inferSchema', True)
.option('multiLine', True)
.option('escape', '"')
.csv(landing_path)
.withColumn('_source_file', input_file_name())
)


# Add ingestion metadata columns
from pyspark.sql.functions import current_timestamp
df = df.withColumn('_ingest_date', current_timestamp())


# Standardize column names: lowercase and replace spaces
for col in df.columns:
df = df.withColumnRenamed(col, col.strip().lower().replace(' ', '_'))


# Write as Delta (append)
df.write.format('delta').mode('append').option('mergeSchema','true').save(bronze_path)


print(f'Wrote {df.count()} rows to {bronze_path}')
