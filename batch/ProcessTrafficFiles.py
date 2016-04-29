from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import Row, StructField, StructType, StringType, IntegerType

import ConfigParser

if __name__ == "__main__":

    sc = SparkContext(appName="SparkContext")

    config = ConfigParser.ConfigParser()
    config.read('configuration.cfg')
    filesInDisk = config.getboolean('BatchProperties', 'FilesInDisk')

    if filesInDisk:
        dirTrafficFiles = config.get('BatchProperties', 'FilesTrafficGeneratedLocal')

    else:
        dirTrafficFiles = config.get('BatchProperties', 'FilesTrafficGeneratedHDFS')

    trafficFiles = sc.textFile(dirTrafficFiles + "*.csv")


    otroRDD = trafficFiles.map(lambda x: x.split('|')).groupBy(lambda x: x[1],x[2],x[3],x[11])
    print(otroRDD.take(10))
