from rddExample import Friends
from sparkSqlExample import FriendsSql
from sparkSqlDataFrame import SparkSql
'''
friends = Friends()
friends.averageByAge()
friends.underHundred()
friends.minFriendsByAge()
friendsSql = FriendsSql()
friendsSql.ageBetween(ageTo=25)
friendsSql.averageFriendsByAge()
friendsSql.spark.stop()
'''

example = SparkSql()
example.minMaxTemp()