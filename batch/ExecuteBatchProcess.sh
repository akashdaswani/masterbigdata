#!/bin/sh
echo "Cargando el Proceso Batch"
spark-submit --jars /opt/spark-1.6.0-bin-hadoop2.6/lib/mongodb-hadoop/mongo-hadoop-spark-1.5.2.jar BatchProcess.py
