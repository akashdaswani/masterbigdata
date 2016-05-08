from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.streaming import StreamingContext
from pyspark.sql.types import Row

from pyspark.mllib.regression import LabeledPoint, LinearRegressionWithSGD, LinearRegressionModel
from common.CommonFunctions import CommonFunctions

from pyspark.streaming.kafka import KafkaUtils
import ConfigParser
import socket


if __name__ == "__main__":

    sc= SparkContext(appName="SparkContext")
    sqlContext = SQLContext(sc)

    config = ConfigParser.ConfigParser()
    config.read('configuration.cfg')

    urlKafkaProducer = config.get('StreamingProperties', 'URLKafkaProducer')
    topicName = config.get('StreamingProperties', 'TopicName')
    filesInDisk = config.getboolean('StreamingProperties', 'FilesInDisk')
    dirStreamingFileLocal = config.get('StreamingProperties', 'StreamingFileLocal')
    mongodb_connection = config.get('StreamingProperties', 'URLMongoDB')

    virtualMachine = 'local'
    if socket.gethostname() == 'ubuntu':
        virtualMachine = socket.gethostname()

    if virtualMachine == 'local':
        dirTrainingModel = config.get('StreamingProperties', 'URLTrainingModelLocal')

    else:
        dirTrainingModel = config.get('StreamingProperties', 'URLTrainingModelHDFS')

    if virtualMachine == 'ubuntu':
        ssc = StreamingContext(sc, 2)
        brokers = "localhost:2181"

        kvs = KafkaUtils.createStream(ssc, \
                                       "localhost:2181", \
                                       topicName,
                                        {"topic":1})


        #kvs = KafkaUtils.createDirectStream (ssc, [topicName], {"metadata.broker.list": brokers})
        kvs.pprint()
        #kvs.foreachRDD (saveData)
        #brokers = "localhost:9092"
        #kvs = KafkaUtils.createDirectStream (ssc, [topicName], {"metadata.broker.list": brokers})
        #KafkaUtils.createStream(scc, )
        #kvs.pprint()
        #kvs.foreachRDD (saveStream)

        #rowData = data.map(lambda row: row.asDict())
        #rowData.saveToMongoDB(mongodb_connection + 'test.resultsStreaming')
        ssc.start()
        ssc.awaitTermination()

    else:
        lines = sc.textFile(dirStreamingFileLocal)
        trafficData = lines.map(lambda p: p.split("|")).map(lambda p: Row(
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

        query = sqlContext.sql("SELECT year, month, day, station, SUM(intensity) intensity  "
                                "FROM traffic "
                                "WHERE error='N' AND station = '28079004' "
                                "GROUP BY year, month, day, station "
                                "LIMIT 1")

        labelPoints = query.map(lambda line:[CommonFunctions.toWeekday(2000 + line[0], line[1], line[2]), CommonFunctions.clasification_intensity(line[4])])
        model = LinearRegressionModel.load(sc, dirTrainingModel)
        valueAir = model.predict(labelPoints.first())

        data = query.map(lambda p: Row(
            valueAir = valueAir,
            year=int(p[0]),
            month=int(p[1]),
            day=int(p[2]),
            station=p[3],
            intensity=p[4]
        ))

        print data.collect()
