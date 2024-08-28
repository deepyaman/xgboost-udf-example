import datafusion
ctx = datafusion.SessionContext()
ctx.register_csv("mushrooms", "./data/mushrooms.csv")
import ibis
con = ibis.datafusion.connect()
mushrooms = con.read_csv('data/mushrooms.csv', "mushrooms")
import ibis_ml as ml
step = ml.OneHotEncode(ml.everything() - ml.cols("class"))
step.fit_table(mushrooms, ml.core.Metadata())
result = step.transform_table(mushrooms)
sql = str(ibis.to_sql(result))
con.disconnect()
import time
iterations = 1000
total = 0
for _ in range(iterations):
    start = time.time()
    df = ctx.sql(sql)
    result = df.collect()
    total += time.time() - start
print("Elapsed:", total / iterations * 1000, "ms")
# print(result)
