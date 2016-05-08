from kafka.producer import KafkaProducer

import ConfigParser
import socket

if __name__ == "__main__":

    config = ConfigParser.ConfigParser()
    config.read('configuration.cfg')

    urlKafkaProducer = config.get('StreamingProperties', 'URLKafkaProducer')
    topicName = config.get('StreamingProperties', 'TopicName')

    virtualMachine = 'local'
    if socket.gethostname() == 'ubuntu':
        virtualMachine = socket.gethostname()

    if virtualMachine == 'local':
        fileName = config.get('StreamingProperties', 'StreamingFileLocal')

    else:
        fileName = config.get('StreamingProperties', 'StreamingFileVirtual')

    producer = KafkaProducer(bootstrap_servers=urlKafkaProducer)

    infile = open (fileName, 'r')
    for line in infile:
        producer.send (topicName, line)
        #time.sleep(0.000000001)

    infile.close()