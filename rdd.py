from pyspark import SparkConf, SparkContext
from os import getcwd
conf = SparkConf().setMaster("local").setAppName("UdemyCourses")
sc = SparkContext(conf=conf)

def parseLine(line):
   """ Function return age and number of friends """
   line = line.split(',')
   return (int(line[2]), int(line[3]))

#lines = sc.textFile(getcwd() + "/*")
lines = sc.textFile(getcwd() + "/Datasets/fakefriends.csv")
friendsByAge = lines.map(parseLine)
totalByAge = friendsByAge.mapValues(lambda x: (x, 1))
sumByAge = friendsByAge.reduceByKey(lambda x, y: x + y).collect()
for record in sorted(sumByAge): print(f"Age: {record[0]}, sum of friends by the age {record[1]}")

averageByAge = totalByAge.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])).mapValues(lambda x: round(x[0] / x[1], 2)).collect()

for record in sorted(averageByAge): print(f"Age: {record[0]}, Average of friends by the age {record[1]}")
