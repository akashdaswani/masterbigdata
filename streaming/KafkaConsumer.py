from kafka import KafkaConsumer
import socket
import ConfigParser

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

    # To consume latest messages and auto-commit offesets
    consumer = KafkaConsumer (topicName,
                              group_id='my_consumer',
                              bootstrap_servers=[urlKafkaProducer])

    for message in consumer:
        # message value and key are raw bytes -- decode if necessary!
        # e.g., for unicode: `message.value.decode('utf-8')`
        print (message)