from __future__ import print_function

import sys
import unicodedata
import time
import sqlite3

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

streamDir = '/home/ubuntu/ejercicios/ArquitecturaLambda/HDFS/new/'
rutaBBDD = '/home/ubuntu/ejercicios/ArquitecturaLambda/views.db'

def saveStream (rdd):
        if (rdd.count() > 0):
            prefix = streamDir + str(time.time())
            rdd.saveAsTextFile(prefix)


def toAscii (cadena):
    return unicodedata.normalize('NFKD', cadena).encode('ascii', 'ignore')


def initDB ():
    conn=sqlite3.connect(rutaBBDD)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS wordcount_rt1 (word, count)''')
    c.execute('''CREATE TABLE IF NOT EXISTS wordcount_rt2 (word, count)''')
    conn.commit()
    c.close()
    conn.close()


def recuperaPalabra (tupla):
    conn=sqlite3.connect(rutaBBDD)
    c = conn.cursor()

    # Hay que pasarle un array con un elemento
    c.execute('SELECT * FROM wordcount_rt1 WHERE word=?', [tupla[0]])
    data = c.fetchall()
    existe = len(data) > 0

    valor = 0
    if existe:
        row = data[0]
        valor = row[1]

    c.close()
    conn.close()
    return valor


def saveOrUpdateBBDD (rdd):
    dato = rdd.collect()


    conn=sqlite3.connect(rutaBBDD)
    c = conn.cursor()

    for i in dato:

        # Recuperamos el valor de la BBDD (si existe)
        valor = recuperaPalabra(i)

        if (valor == 0):
            c.execute ('INSERT INTO wordcount_rt1 VALUES (?,?)', i)
            print ("Creando fila en wordcount_rt1 " + str(i))

        else:
            update = []
            update.append(i[0])
            update.append(i[1] + valor)
            c.execute('UPDATE wordcount_rt1 SET count = ? WHERE word = ?', update)
            print ("Actualizando fila en wordcount_rt1 " + str(update))

        conn.commit()

    c.close()
    conn.close()

if __name__ == "__main__":

    sc = SparkContext(appName="PythonStreamingKafka")
    ssc = StreamingContext(sc, 5)


    #conexion a Kafka
    brokers = "localhost:9092"
    topic = "pythontest"
    kvs = KafkaUtils.createDirectStream (ssc, [topic], {"metadata.broker.list": brokers})

    # El KVS es un DStream de 5 elementos.
    # Cada uno de estos elementos es un RDD, que se compone de lineas de texto.

    # KVS tiene un forEachRDD, donde le podemos pasar una funcion logica (que puede recibir un RDD)

    # Imprimimos
    # kvs.pprint()

    #-------------------------------------------
    #Time: 2016-02-20 09:28:40
    #-------------------------------------------
    #(None, u'de L\xf3pez Maldonado. Tambi\xe9n el autor de ese libro, replic\xf3 el\n')
    #(None, u'cura, es grande amigo m\xedo, y sus versos en su boca admiran a\n')
    #(None, u'quien los oye, y tal es la suavidad de la voz con que los canta,\n')

    # Hacemos un update
    toHDFS=kvs
    toLogic=kvs

    # Guardar en HDFS (en este caso solo en memoria)
    toHDFS.foreachRDD (saveStream)

    # Inicializamos la BBDD
    initDB()

    # El map contiene la linea que viene de Kafka
    counts = toLogic.map(lambda  x: x[1]) \
             .flatMap(lambda line: line.split(" ")) \
             .map(lambda wordAscii: (toAscii(wordAscii))) \
             .map(lambda word: (word, 1)) \
             .reduceByKey(lambda a, b: a + b)

    # counts.pprint()
    #-------------------------------------------
    #Time: 2016-02-20 09:28:40
    #-------------------------------------------
    #(u'Tambi\xe9n', 1)
    #(u'dijo', 1)
    #(u'L\xf3pez', 1)
    #(u'y\n', 1)
    #(u'la', 3)

    # Guardamos en BBDD
    counts.foreachRDD(saveOrUpdateBBDD)

    ssc.start()
    ssc.awaitTermination()
