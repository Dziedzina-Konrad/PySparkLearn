from pyspark import SparkConf, SparkContext
from os import getcwd

def parseLine(line):
   """ Function return age, number of friends and name of record """
   line = line.split(',')
   return (int(line[2]), int(line[3]), line[1])

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
      
   def underAge(self, age = 100):
      """ Return all friends under selected age. Default  100 """
      if type(age) != int:
         raise TypeError("Age can only be a int.")
      self.underAge = self.friendsByAge.filter(lambda x: x[1] < age).collect()
      for record in sorted(self.underAge): print(f"Name: {record[2]}, age: {record[0]}, friends: {record[1]}")
   
   def minFriendsByAge(self):
      """ Return minimum of friends by age. """
      minFriends =  self.friendsByAge.map(lambda x: (x[0], x[1])).reduceByKey(lambda x, y: min(x,y)).collect()
      for record in sorted(minFriends): print(f"Age: {record[0]}, min friends {record[1]}")