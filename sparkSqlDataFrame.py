from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, IntegerType
from os import getcwd

schema = StructType([
                  StructField("Station", StringType(), True),
                  StructField("Date", StringType(), True),
                  StructField("Measures", StringType(), True),
                  StructField("Temp", IntegerType(), True)
                  ])

spark = SparkSession.builder.appName('tempMeasures').getOrCreate()
df = spark.read.schema(schema).csv(getcwd() + '/Datasets/1800.csv')
df.createOrReplaceTempView('stationTemp')
spark.sql('SELECT * FROM stationTemp').show(50)