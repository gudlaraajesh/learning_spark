from pyspark import SparkContext

sc = SparkContext(master="local", appName="Movie rating counts")

file = sc.textFile("/workspaces/learning_spark/moviedata-201008-180523.data")
ratings = file.map(lambda x: x.split("\t")[2])
ratings_count = ratings.countByValue()
print(ratings_count)