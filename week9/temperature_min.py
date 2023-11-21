from pyspark import SparkContext
import datetime
sc = SparkContext(master="local", appName='Min temperature')

def split_line(line):
    words = line.split(",")
    return (words[0], words[3])

start_time = datetime.datetime.now()
input_file = sc.textFile("/workspaces/learning_spark/week9/tempdata-201125-161348.csv")
get_id_temp = input_file.map(lambda x: split_line(x))
id_min_temp = get_id_temp.reduceByKey(lambda x, y: min(float(x),float(y)))
results = id_min_temp.collect()
end_time = datetime.datetime.now()
print(start_time, end_time)
print(f'Time taken: {0}'.format((end_time - start_time)))
for res in results:
    print(res)