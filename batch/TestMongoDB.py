from pyspark import SparkContext, SparkConf
from pymongo import MongoClient


import pymongo_spark
import os.path
import ConfigParser
import pymongo

# Importante: activate pymongo_spark.
pymongo_spark.activate()

def main():
    conf = SparkConf().setAppName("pyspark test")
    sc = SparkContext(conf=conf)

    config = ConfigParser.ConfigParser()
    config.read('configuration.cfg')
    mongodb_connection = config.get('BatchProperties', 'URLMongoDB')

    #######################################################
    # UTILIZACION DE LA LIBRERIA DE PYMONGO
    #######################################################
    client = MongoClient()
    db = client.test

    cursor = db.tabla1.find()

    for document in cursor:
        print(document)


    #######################################################
    # UTILIZACION DE LA LIBRERIA DE pymongo_spark
    #######################################################
    # Lectura de una tabla de mongodb (db: test; coleccion: tabla1)
    rdd = sc.mongoRDD(mongodb_connection + 'test.tabla1')

    # Guardamos el rdd leido en mongodb (db: test; coleccion: tabla2)
    rdd.saveToMongoDB(mongodb_connection + 'test.tabla2')

    # Recuperamos el valor de raiz del proyecto
    BASE_DIR = os.path.dirname(os.path.dirname(__file__))
    # BASE_DIR = /Users/akash/PycharmProjects/masterbigdata

    # Leemos un fichero de ejemplo
    file = os.path.join(BASE_DIR + '/datasets/batch/calidadaire', 'ficheroSalida.txt')
    rddfFile = sc.textFile(file)

     # Almancemos en mongodb el fichero
    rddfFile.saveToMongoDB(mongodb_connection + 'test.tabla3')

if __name__ == '__main__':
    main()

