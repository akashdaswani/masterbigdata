from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import Row

sc= SparkContext(appName="SparkContext")
sqlContext = SQLContext(sc)

airFiles = sc.textFile("/Users/akash/PycharmProjects/masterbigdata/datasets/batch_processing/air/*.txt")
airData = airFiles.map(lambda p: p.split("|")).map(lambda p: Row(
    product=p[0],
    road=p[1],
    station=p[2],
    measurement=int(p[3]),
    type=int(p[4]),
    time_format=int(p[5]),
    year=int(p[6]),
    month=int(p[7]),
    day=int(p[8]),
    value=int(p[9])
))

airDF = sqlContext.createDataFrame(airData)
airDF.registerTempTable("air")

query = sqlContext.sql("SELECT distinct station FROM air WHERE road = 'Plaza de Espana'")
stations = query.map(lambda p: "Name: " + p.station)
for station in stations.collect():
  print(station)

print query.collect()
print query.printSchema()
