from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import Row, StructField, StructType, StringType, IntegerType
from pyspark.mllib.regression import LinearRegressionWithSGD, LabeledPoint

import ConfigParser


# Load and parse the data
def parsePoint(line):
    print "Linea: " + str(line)
    values = [int(x) for x in line.replace(',', ' ').split(' ')]
    return LabeledPoint(values[0], values[1:])


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


    query = sqlContext.sql("SELECT a.valueAir, a.station, a.year, a.month, a.day, t.intensity "
                           "FROM ("
                                "SELECT year, month, day, station, SUM(value) valueAir "
                                "FROM air "
                                "GROUP BY year, month, day, station"
                           ") a INNER JOIN ("
                                "SELECT year, month, day, station, SUM(intensity) intensity  "
                                "FROM traffic "
                                "WHERE error='N' "
                                "GROUP BY year, month, day, station "
                           ") t ON a.year = t.year "
                                "AND a.month = t.month "
                                "AND a.day = t.day "
                                "AND a.station = t.station "
                           "")

    # WHERE a.station = '28079004'
    for t in query.collect():
        print str(t)

    #temp = query.map(lambda line:LabeledPoint(line[0],[line[1:]]))
    #temp.take(5)

    # Build the model
    #model = LinearRegressionWithSGD.train(temp, iterations=10, step=0.0000001)

    #model.predict(temp)

    # Evaluate the model on training data
    #valuesAndPreds = temp.map(lambda p: (p.label, model.predict(p.features)))
    #MSE = valuesAndPreds.map(lambda (v, p): (v - p)**2).reduce(lambda x, y: x + y) / valuesAndPreds.count()
    #print("Mean Squared Error = " + str(MSE))

    # Save and load model
    #model.save(sc, "/Users/akash/PycharmProjects/masterbigdata/datasets/batch_processing/")
