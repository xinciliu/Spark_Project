import time
###architecture 1
df = spark.read.format("csv").option("header", "true").load(file)
print(time.time())
df.groupby('tradeCurrency').count()
print(time.time())

###architecture 2
df = spark.read.format("csv").option("header", "true").load(file)
print(time.time())
sql_query='Select tradeCurrency, Count(TradeId)as num_of_trades From transactions Group by tradeCurrency'
df.createOrReplaceTempView("transactions")
results = spark.sql(sql_query)
print(time.time())
