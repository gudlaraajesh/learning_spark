from pyspark import SparkContext

def split_func(row):
    split_row = row.split("::")
    return (split_row[2], (int(split_row[3]),1))
    # return (split_row[2], int(split_row[3]))

sc = SparkContext(master="local", appName="Friends average count")

file = sc.textFile("/workspaces/learning_spark/friendsdata-201008-180523.csv")

age_list = file.map(lambda x: split_func(x))

# age_list = age_list.mapValues(lambda x: (x,1))

friends_count = age_list.reduceByKey(lambda x, y: (x[0]+ y[0], x[1] + y[1]))

# avg_friends = friends_count.map(lambda x: (x[0],x[1][0]/x[1][1]))
avg_friends = friends_count.mapValues(lambda x: (x[0]/x[1]))

results = avg_friends.sortBy(lambda x: x[1],ascending=False).collect()

for res in results:
    print(res)


