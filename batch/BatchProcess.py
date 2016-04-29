from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import Row, StructField, StructType, StringType, IntegerType

import ConfigParser

if __name__ == "__main__":

    sc = SparkContext(appName="SparkContext")
    sqlContext = SQLContext(sc)

    config = ConfigParser.ConfigParser()
    config.read('configuration.cfg')
    filesInDisk = config.getboolean('BatchProperties', 'FilesInDisk')

    if filesInDisk:
        dirAirFiles = config.get('BatchProperties', 'FilesAirGeneratedLocal')
        dirTrafficFiles = config.get('BatchProperties', 'FilesTrafficGeneratedLocal')

    else:
        dirAirFiles = config.get('BatchProperties', 'FilesAirGeneratedHDFS')
        dirTrafficFiles = config.get('BatchProperties', 'FilesTrafficGeneratedHDFS')

    airFiles = sc.textFile(dirAirFiles + "*.txt")
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

    #identif|ano|mes|dia|intensidad|ocupacion|carga|tipo|vmed|error|periodo_integracion|estacion
    #PM20152|2013|07|12|1065|9|48|M|73|N|4|28079024
    trafficFiles = sc.textFile(dirTrafficFiles + "*.csv")
    trafficData = trafficFiles.map(lambda p: p.split("|")).map(lambda p: Row(
        identif=p[0],
        year=int(p[1]),
        month=int(p[2]),
        day=int(p[3]),
        intensity=int(p[4]),
        occupation=int(p[5]),
        load=int(p[6]),
        type=p[7],
        velocity=int(p[8]),
        error=p[9],
        integration=int(p[10]),
        station=p[11]
    ))

    trafficDF = sqlContext.createDataFrame(trafficData)
    trafficDF.registerTempTable("traffic")


    query = sqlContext.sql("SELECT a.year, a.month, a.day, a.station, a.valueAir, t.intensity "
                           "FROM ("
                                "SELECT year, month, day, station, SUM(value) valueAir "
                                "FROM air "
                                "GROUP BY year, month, day, station"
                           ") a LEFT JOIN ("
                                "SELECT year, month, day, station, SUM(intensity) intensity "
                                "FROM traffic "
                                "GROUP BY year, month, day, station "
                           ") t ON a.year = t.year "
                                "AND a.month = t.month "
                                "AND a.day = t.day "
                                "AND a.station = t.station ")

    #query = sqlContext.sql("SELECT year, month, day, station, SUM(value) "
    #                       "FROM air "
    #                       "GROUP BY year, month, day, station ")


    #query = sqlContext.sql("SELECT year, month, day, station, SUM(intensity) "
    #                       "FROM traffic "
    #                       "GROUP BY year, month, day, station ")

    for t in query.collect():
        print str(t)


    #query = sqlContext.sql("SELECT t.year, t.month, t.day, a.product, t.station, t.measurement, a.intensity "
    #                       "FROM traffic t, air a "
    #                       "WHERE t.year = a.year "
    #                       "AND t.month = a.month "
    #                       "AND t.day = a.day "
    #                       "AND t.station = a.station")

    #for t in query.collect():
    #    print str(t.year) + "|" + str(t.month) + "|" + str(t.day) \
    #          + "|" + str(t.station) + "|" + str(t.measurement) \
    #          + "|" + str(t.intensity)