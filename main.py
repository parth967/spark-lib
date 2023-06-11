from pyspark.sql import SparkSession
from spark.spark_init import SparkInitialization
import os


spark_init = SparkInitialization()
spark = spark_init._init_spark()
current_directory = os.getcwd()

delta_path = os.path.join(current_directory, 'data/delta_tables')
prev_version = 0
current_version = 1

main_table = f'{delta_path}/maint1'
cloned_table = f'{delta_path}/cloned1'

data = [("dsd", 11), ("ccc", 12), ("dssd", 13),("dcsdc", 14), ("dvsd", 15), ("asdsad", 16)]
df = spark.createDataFrame(data, ["Name", "Age"])

df.write.format('delta').mode("append").save(main_table)

# select_query = f'''SELECT `Name`, `Age`, current_timestamp() as time_date_load from 
#                          delta.`{main_table}`
#                      '''
# previous_metadata = spark.sql(f"DESCRIBE HISTORY delta.`{main_table}`")
# current_metadata = spark.sql(f"DESCRIBE HISTORY delta.`{main_table}`")

# #spark.sql(current_metadata).show()


df = spark.read.format("delta") \
  .option("readChangeFeed", "true") \
  .option("startingVersion", 10) \
  .option("endingVersion", 11) \
  .load(main_table)

df.createOrReplaceTempView("cdc_f")
df.show()

df = spark.sql(f"select *, current_timestamp() as time_date_load from delta.`{main_table}` where Age in (select distinct Age from cdc_f)")

df.select

delta_table = spark.read.format("parquet").load(cloned_table)

#delta_table.repartitionByRange('Age').write.format("parquet").mode("overwrite").save(cloned_table)

#spark.read.format("parquet").load(cloned_table).show()
spark.conf.set('spark.sql.sources.partitionOverwriteMode', 'dynamic')
df.write.format("parquet").partitionBy("Age").mode("overwrite").save(cloned_table)

#spark.read.format("parquet").load(cloned_table).show()

curr = f'select Age, Count(*) FROM delta.`{main_table}` group by Age'
abc = spark.sql(curr)
abc.show()

cd = spark.read.format('parquet').load(cloned_table)
dvd = cd.select("Age").groupBy("Age").count()
dvd.show()

asd = spark.read.format('parquet').load(cloned_table)
ccn = asd.select("time_date_load").groupBy("time_date_load").count()
ccn.show()

