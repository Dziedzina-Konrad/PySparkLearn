from pyspark import SparkConf, SparkContext
from os import getcwd

def parseLine(line):
   """ Function return age and number of friends """
   line = line.split(',')
   return (int(line[2]), int(line[3]))

class Friends:
   def __init__(self):
      self.conf = SparkConf().setMaster("local").setAppName("UdemyCourses")
      self.sc = SparkContext(conf=self.conf)
      self.lines = self.sc.textFile(getcwd() + "/Datasets/fakefriends.csv")
      self.friendsByAge = self.lines.map(parseLine)
      
   def sumByAge(self):
      """ Return sum of friends by age in sorted order. """
      self.sumByAge = self.friendsByAge.reduceByKey(lambda x, y: x + y).collect()
      for record in sorted(self.sumByAge): print(f"Age: {record[0]}, sum of friends by the age {record[1]}")
      
   def averageByAge(self):
      """ Return average of friends by age in sorted order. """
      totalByAge = self.friendsByAge.mapValues(lambda x: (x, 1))
      self.averageByAge = totalByAge.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])).mapValues(lambda x: round(x[0] / x[1], 2)).collect()
      for record in sorted(self.averageByAge): print(f"Age: {record[0]}, Average of friends by the age {record[1]}")
      
   def underHundret(self):
      self.belowHundret = self.friendsByAge.filter(lambda x: x[1] < 100).collect()
      for record in sorted(self.belowHundret): print(f"Age: {record[0]}, Average of friends by the age {record[1]}")
      