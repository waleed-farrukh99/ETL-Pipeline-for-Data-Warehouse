from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


spark = SparkSession.builder.getOrCreate()


silver_path = '/mnt/dw_silver/sales'
conform_path = '/mnt/dw_silver/conformed'


df = spark.read.format('delta').load(silver_path)


# Build dim_customer (simple heuristic: customer_id, name fields if exist)
cust_cols = ['customer_id', 'customer_name', 'customer_email', 'customer_phone']
available = [c for c in cust_cols if c in df.columns]
if 'customer_id' in df.columns:
dim_customer = df.select([c for c in available]).dropDuplicates(['customer_id'])
dim_customer = dim_customer.withColumn('customer_surrogate_id', F.monotonically_increasing_id())
dim_customer.write.format('delta').mode('overwrite').save(f'{conform_path}/dim_customer')


# Build dim_product
prod_cols = ['product_id', 'product_name', 'category']
avail_p = [c for c in prod_cols if c in df.columns]
if 'product_id' in df.columns:
dim_product = df.select(avail_p).dropDuplicates(['product_id'])
dim_product = dim_product.withColumn('product_surrogate_id', F.monotonically_increasing_id())
dim_product.write.format('delta').mode('overwrite').save(f'{conform_path}/dim_product')


# Build dim_store
store_cols = ['store_id', 'store_name', 'store_region']
avail_s = [c for c in store_cols if c in df.columns]
if 'store_id' in df.columns:
dim_store = df.select(avail_s).dropDuplicates(['store_id'])
dim_store = dim_store.withColumn('store_surrogate_id', F.monotonically_increasing_id())
dim_store.write.format('delta').mode('overwrite').save(f'{conform_path}/dim_store')


# Build dim_date (from transaction_date)
if 'transaction_date' in df.columns:
dim_date = (df.select(F.col('transaction_date').alias('date'))
.where(F.col('date').isNotNull())
.dropDuplicates(['date'])
.withColumn('year', F.year('date'))
.withColumn('month', F.month('date'))
.withColumn('day', F.dayofmonth('date'))
.withColumn('date_surrogate_id', F.monotonically_increasing_id())
)
dim_date.write.format('delta').mode('overwrite').save(f'{conform_path}/dim_date')


# Prepare fact staging: join to surrogate ids where possible
fact_staging = df


# left-join on product_id -> product_surrogate_id
if 'product_id' in df.columns:
fact_staging.write.format('delta').mode('overwrite').save(f'{conform_path}/fact_staging')
