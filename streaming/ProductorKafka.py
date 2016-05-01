#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import time
import urllib2
import ssl
from urllib2 import urlopen
from kafka.client import KafkaClient
from kafka.producer import KafkaProducer

producer = KafkaProducer(bootstrap_servers="localhost:9092")

data = urllib2.urlopen("http://www.mambiente.munimadrid.es/opendata/horario.txt")

for line in data:
    producer.send("test",line)
    print line
    time.sleep(0.01)
