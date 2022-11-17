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
spark.sql('''SELECT DISTINCT Station, 
          MIN(Temp) OVER(PARTITION BY Station, Measures) AS MinTemp,
          MAX(Temp) OVER(PARTITION BY Station, Measures) AS MinTemp,
          FIRST_VALUE(DATE) OVER(PARTITION BY Station, Measures ORDER BY Temp) AS DateOfMinTemp,
          FIRST_VALUE(DATE) OVER(PARTITION BY Station, Measures ORDER BY Temp DESC) AS DateOfMaxTemp
          FROM stationTemp
          ORDER BY Station''').show(100)