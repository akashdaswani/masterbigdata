#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pyspark import SparkContext
from pyspark.mllib.regression import LabeledPoint, LinearRegressionWithSGD, LinearRegressionModel


import ConfigParser

# Load and parse the data
def parsePoint(line):
    values = [float(x) for x in line.split('|')]
    return LabeledPoint(values[0], values[1:])


if __name__ == "__main__":
    sc= SparkContext(appName="SparkContext")

    config = ConfigParser.ConfigParser()
    config.read('configuration.cfg')

    data = sc.textFile("/Users/akash/PycharmProjects/masterbigdata/datasets/training/data/result.txt")
    parsedData = data.map(parsePoint)
    parsedData.saveAsTextFile("/Users/akash/PycharmProjects/masterbigdata/datasets/training/data/labeledPoints")

    # Build the model
    model = LinearRegressionWithSGD.train(parsedData, iterations=100, step=0.00000001)

    # Save and load model
    model.save(sc, "/Users/akash/PycharmProjects/masterbigdata/datasets/training/model")
