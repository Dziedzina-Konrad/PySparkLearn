from pyspark.sql import SparkSession, Row
from os import getcwd

def mapper(line):
   fields = line.split(',')
   return Row(ID=int(fields[0]),
               Name=str(fields[1]),
               Age=int(fields[2]),
               Friends=int(fields[3]))

spark = SparkSession.builder.appName("SqlExample").getOrCreate()

lines = spark.sparkContext.textFile(getcwd() + "/Datasets/fakefriends.csv")
friends = lines.map(mapper)

schemaFriends = spark.createDataFrame(friends).cache()
schemaFriends.createOrReplaceTempView("friends")

teenagers = spark.sql("SELECT * FROM friends WHERE Age BETWEEN 13 AND 19")

for teen in teenagers.collect():
   print(teen)
spark.stop()