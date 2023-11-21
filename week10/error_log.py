from pyspark import SparkContext

sc = SparkContext(master="local[*]", appName="Error Log count")

lst = ["ERROR: This is first error", "INFO: This is Info message",
       "ERROR: This is first error","INFO: This is Info message","ERROR: This is first error",]

lst_rdd = sc.parallelize(lst)
split_rdd = lst_rdd.map(lambda x: (x.split(":")[0],1))
reduce_rdd = split_rdd.reduceByKey(lambda x, y: x + y)
results = reduce_rdd.collect()

for res in results:
    print(res)