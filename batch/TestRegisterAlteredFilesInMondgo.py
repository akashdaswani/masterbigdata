#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import Row

import pymongo_spark
import ConfigParser
import os.path


# Importante: activate pymongo_spark.
pymongo_spark.activate()

if __name__ == "__main__":
    sc= SparkContext(appName="SparkContext")

    sqlContext = SQLContext(sc)

    config = ConfigParser.ConfigParser()
    config.read('configuration.cfg')

    filesInDisk = config.getboolean('BatchProperties', 'FilesInDisk')
    mongodb_connection = config.get('BatchProperties', 'URLMongoDB')


    # Recuperamos el valor de raiz del proyecto
    BASE_DIR = os.path.dirname(os.path.dirname(__file__))


    rawDataTrafico = sc.textFile(BASE_DIR + "/datasets/batch/ficherosTratados/ficheroSalidaTrafico.txt")
    dataTrafico = rawDataTrafico.map(lambda p: p.split("|"))

    #PM20152|2013|01|01|1065|9|48|M|73|N|4|28079004
    rowDataTrafico = dataTrafico.map(lambda p: Row(
        idelem=p[0], # PM20152
        ano=int(p[1]), # 2013
        mes=int(p[2]), # 01
        dia=int(p[3]), # 01
        intensidad=int(p[4]), # 1065
        ocupacion=int(p[5]), # 9
        carga=int(p[6]), # 48
        tipo=p[7], # M
        velocidadMedia=float(p[8]), # 73
        error=p[9], # N
        periodoIntegracion=int(p[10]), # 4
        codigoEstacion=p[11], # 28079004
    ))

    config = ConfigParser.ConfigParser()
    config.read('configuration.cfg')
    mongodb_connection = config.get('BatchProperties', 'URLMongoDB')

    rowDataTrafico = rowDataTrafico.map(lambda row: row.asDict())
    rowDataTrafico.saveToMongoDB(mongodb_connection + 'test.trafico')


    rawDataAire = sc.textFile(BASE_DIR + "/datasets/batch/ficherosTratados/ficheroSalidaAire.txt")
    dataAire = rawDataAire.map(lambda p: p.split("|"))

    #Dioxido de Azufre|Plaza de Espana|28079004|2013|01|01|30
    rowDataAire = dataAire.map(lambda p: Row(
        magnitud=p[0], # Dioxido de Azufre
        localizacion=p[1], # Plaza de Espana
        codigoEstacion=p[2], # 28079004
        ano=int(p[3]), # 2013
        mes=int(p[4]), # 01
        dia=int(p[5]), # 01
        valor=int(p[6]) # 30
    ))

    rowDataAire = rowDataAire.map(lambda row: row.asDict())
    rowDataAire.saveToMongoDB(mongodb_connection + 'test.calidadaire')