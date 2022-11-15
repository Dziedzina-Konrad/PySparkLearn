from pyspark.sql import SparkSession, Row
from os import getcwd

def mapper(line):
   fields = line.split(',')
   return Row(ID=int(fields[0]),
               Name=str(fields[1]),
               Age=int(fields[2]),
               Friends=int(fields[3]))

class FriendsSql:
   def __init__(self):
      self.spark = SparkSession.builder.appName("SqlExample").getOrCreate()
      self.lines = self.spark.sparkContext.textFile(getcwd() + "/Datasets/fakefriends.csv")
      self.friends = self.lines.map(mapper)
      self.schemaFriends = self.spark.createDataFrame(self.friends).cache()
      self.schemaFriends.createOrReplaceTempView("friends")

   def ageBetween(self, ageFrom = 13, ageTo=18):
      self.spark.sql(f"SELECT * FROM friends WHERE Age BETWEEN {ageFrom} AND {ageTo} ORDER BY Age").show(100)
   
   def averageFriendsByAge(self):
      self.schemaFriends.groupBy('Age').avg('Friends').orderBy('Age').show(50)