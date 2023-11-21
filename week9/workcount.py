from pyspark import SparkContext # SparkSession, not avaiable here
# from pyspark.sql import  SparkSession # SparkContext, not available here
sc = SparkContext(master="local",appName="Word Count")

file = sc.textFile("/workspaces/learning_spark/wordcount.txt")
split_lines = file.flatMap(lambda x : x.split(" "))

words = split_lines.map(lambda x : (x.lower(),1))
words_count = words.reduceByKey(lambda x, y : (x+y))

# words = split_lines.map(lambda x : (x.lower()))
# words_count = words.countByValue()
# print(words_count)


# words_count = sc.textFile("/workspaces/learning_spark/wordcount.txt")\
#                 .flatMap(lambda x : x.split(" "))\
#                 .map(lambda x : (x.lower(),1))\
#                 .reduceByKey(lambda x, y : x + y)

results = words_count.collect()
for res in results:
    print(res)



# words_count.saveAsTextFile("/workspaces/learning_spark/wordcount_output")
