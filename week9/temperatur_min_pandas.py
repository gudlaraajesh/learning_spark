import pandas as pd
import datetime

start_time = datetime.datetime.now()
header_cols = ["id","1","2","temperature","3","4","5","6"]
df = pd.read_csv('/workspaces/learning_spark/week9/tempdata-201125-161348.csv',names=header_cols)
df = df[['id','temperature']]
df["id"] = pd(df["y"])
print(df.id.dtype)

df.groupby('id').min()
end_time = datetime.datetime.now()
print(start_time, end_time)
print(f'Time taken: {0}'.format((end_time - start_time)))
print(df)

