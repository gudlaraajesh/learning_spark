from pyspark import SparkContext

sc = SparkContext(master= "local", appName="Top 5 Words")
sc.setLogLevel("ERROR")

file = sc.textFile('/workspaces/learning_spark/wordcount.txt')
split_lines = file.flatMap(lambda x : x.split(" "))
words = split_lines.map(lambda x: (x.lower(),1))
words_count = words.reduceByKey(lambda x, y: x + y)

# reverse_key_words = words_count.map(lambda x: (x[1], x[0]))
# sort_keys = reverse_key_words.sortByKey(ascending=False)
# sort_words = sort_keys.map(lambda x: (x[1], x[0]))

sort_words = words_count.sortBy(lambda x : x[1], ascending= False)

results = sort_words.collect()

for res in results:
    print('"{}" with count {}'.format(res[0], res[1]))

for res in results:
    print('"{}" with count {}'.format(res[0], res[1]))