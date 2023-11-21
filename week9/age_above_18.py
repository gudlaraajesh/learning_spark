from pyspark import SparkContext

sc = SparkContext(master="local[*]", appName="Age above 18")

input_file = sc.textFile("/workspaces/learning_spark/week9/dataset1")
age = input_file.map(lambda x: (x,'Y' if int(x.split(",")[1]) >18 else 'N'))
results = age.map(lambda x: ",".join(x)).collect()
for res in results:
    print(res)