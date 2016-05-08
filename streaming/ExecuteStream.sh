#!/bin/sh
echo "Cargando el proceso de Streaming"
spark-submit --jars /opt/spark-1.6.0-bin-hadoop2.6/lib/spark-streaming-kafka-assembly_2.10-1.5.2.jar /u02/proyecto/masterbigdata/streaming/Stream.py
