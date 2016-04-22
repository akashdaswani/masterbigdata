from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.types import *

import os.path
import ConfigParser
import pymongo
import json
import pymongo_spark


pymongo_spark.activate()

def main():
    conf = SparkConf()\
        .setAppName("pyspark test")

    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    config = ConfigParser.ConfigParser()
    config.read('configuration.cfg')
    mongodb_connection = config.get('BatchProperties', 'URLMongoDB')

    #######################################################
    # UTILIZACION DE LA LIBRERIA DE Stratio
    #######################################################

    # Recuperamos el valor de raiz del proyecto
    BASE_DIR = os.path.dirname(os.path.dirname(__file__))

    # Leemos un fichero de ejemplo
    df = sqlContext.read.json(BASE_DIR + '/datasets/batch/calidadaire/ficheroSalida.json')

    df.saveToMongodb()


if __name__ == '__main__':
    main()

