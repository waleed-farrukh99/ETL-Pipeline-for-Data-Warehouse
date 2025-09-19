from pyspark.sql import SparkSession
from pyspark.sql import functions as F


spark = SparkSession.builder.getOrCreate()
conform_path = '/mnt/dw_silver/conformed'
gold_path = '/mnt/dw_gold'


# Move conformed deltas to gold final table locations with canonical names
spark.read.format('delta').load(f'{conform_path}/dim_customer').write.format('delta').mode('overwrite').save(f'{gold_path}/dim_customer')
spark.read.format('delta').load(f'{conform_path}/dim_product').write.format('delta').mode('overwrite').save(f'{gold_path}/dim_product')
spark.read.format('delta').load(f'{conform_path}/dim_store').write.format('delta').mode('overwrite').save(f'{gold_path}/dim_store')
spark.read.format('delta').load(f'{conform_path}/dim_date').write.format('delta').mode('overwrite').save(f'{gold_path}/dim_date')
