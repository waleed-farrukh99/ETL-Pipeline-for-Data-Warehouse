from pyspark.sql import SparkSession
from pyspark.sql import functions as F


spark = SparkSession.builder.getOrCreate()
bronze_path = '/mnt/dw_bronze/sales'
silver_path = '/mnt/dw_silver/sales'


# Read bronze
df = spark.read.format('delta').load(bronze_path)


# Basic cleaning rules (apply conservative rules):
# - Trim strings
# - Fix numeric columns
# - Parse dates


str_cols = [c for c, t in df.dtypes if t.startswith('string')]
for c in str_cols:
df = df.withColumn(c, F.trim(F.col(c)))


# Example normalization: rename common columns if present
renames = {
'transaction_id': 'transaction_id',
'transaction_date': 'transaction_date',
'product_id': 'product_id',
'qty': 'quantity',
'price': 'unit_price',
'store_id': 'store_id',
'customer_id': 'customer_id'
}
for src, dst in renames.items():
if src in df.columns and src != dst:
df = df.withColumnRenamed(src, dst)


# Cast numeric columns safely
numeric_map = {'quantity':'int', 'unit_price':'double'}
for col, dtype in numeric_map.items():
if col in df.columns:
df = df.withColumn(col, F.col(col).cast(dtype))


# Parse date (assume yyyy-MM-dd or common formats)
if 'transaction_date' in df.columns:
df = df.withColumn('transaction_date', F.to_date('transaction_date'))


# Deduplicate based on transaction_id + product_id if transaction_id exists
if 'transaction_id' in df.columns:
window_cols = ['transaction_id']
df = df.dropDuplicates(subset=window_cols + ['product_id'])
else:
df = df.dropDuplicates()


# Add a surrogate _bronze_id
from pyspark.sql.functions import monotonically_increasing_id
if '_bronze_id' not in df.columns:
df = df.withColumn('_bronze_id', monotonically_increasing_id())


# Write to silver as Delta (merge schema)
df.write.format('delta').mode('overwrite').option('overwriteSchema','true').save(silver_path)
