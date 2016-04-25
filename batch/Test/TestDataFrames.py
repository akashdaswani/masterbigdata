from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import Row

sc= SparkContext(appName="SparkContext")
sqlContext = SQLContext(sc)


data_file = "C:/Users/pvaquero/Desktop/KSCHOOL/PROYECTOS/ficherosGenerados/ficheroSalida.txt"
raw_data = sc.textFile(data_file).cache()

csv_data = raw_data.map(lambda p: p.split("|"))
row_data = csv_data.map(lambda p: Row(
    product=p[0],
    road=p[1],
    estation=p[2],
    measurement=int(p[3]),
    type=int(p[4]),
    time_format=int(p[5]),
    year=int(p[6]),
    month=int(p[7]),
    value=int(p[8]),
    days=int(p[9])
))

print row_data.take(1)

interactions_df = sqlContext.createDataFrame(row_data)
interactions_df.registerTempTable("air_measurement")

tcp_interactions = sqlContext.sql("""
               SELECT type FROM air_measurement WHERE road = 'Plaza de Espana'
           """)

print tcp_interactions.collect()
