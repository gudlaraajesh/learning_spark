from pyspark import SparkContext

def split_func(row):
    split_row = row.split(",")
    return (split_row[0], float(split_row[2]))



sc = SparkContext(master="local", appName="Top Customers")

file = sc.textFile('/workspaces/learning_spark/customerorders-201008-180523.csv')

cust_data = file.map(lambda x: split_func(x))

cust_expenses = cust_data.reduceByKey(lambda x, y: x + y)
top_cust = cust_expenses.sortBy(lambda x: x[1], ascending= False)
results = top_cust.collect()

for res in results:
    print(res)