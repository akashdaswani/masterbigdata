#!/bin/sh
echo "Cargando el test de MongoStratioDB......."
echo "Elimianmos las librerias: ,/opt/spark-1.6.0-bin-hadoop2.6/lib/mongodb/spark-mongodb-core-0.8.7.jar,/opt/spark-1.6.0-bin-hadoop2.6/lib/mongodb/casbah-commons_2.10-2.8.0.jar,/opt/spark-1.6.0-bin-hadoop2.6/lib/mongodb/casbah-core_2.10-2.8.0.jar,/opt/spark-1.6.0-bin-hadoop2.6/lib/mongodb/casbah-query_2.10-2.8.0.jar,/opt/spark-1.6.0-bin-hadoop2.6/lib/mongodb/mongo-java-driver-2.13.0.jar"
spark-submit --jars /opt/spark-1.6.0-bin-hadoop2.6/lib/mongodb-hadoop/mongo-hadoop-spark-1.5.2.jar BatchProcess.py
