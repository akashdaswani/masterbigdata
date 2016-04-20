#!/bin/sh
echo "Cargando el test de MongoDB......."
spark-submit --jars /opt/spark-1.6.0-bin-hadoop2.6/lib/mongo-hadoop-spark-1.5.2.jar TestMongoDB.py
