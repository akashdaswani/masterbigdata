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

    model = LinearRegressionModel.load(sc, "/Users/akash/PycharmProjects/masterbigdata/datasets/training/model")
    prediccionAire = model.predict([28079011.0,13.0,10.0,1.0,10339.0])
    print prediccionAire

    data = sc.textFile("/Users/akash/PycharmProjects/masterbigdata/datasets/training/data/result.txt")

    parsedData = data.map(parsePoint)

    valuesAndPreds = parsedData.map(lambda p: (p.label, model.predict(p.features)))
    print ("valuesAndPreds: " + str(valuesAndPreds))
    valuesAndPreds.saveAsTextFile("/Users/akash/PycharmProjects/masterbigdata/datasets/training/results")

    MSE = valuesAndPreds.map(lambda (v, p): (v - p)**2).reduce(lambda x, y: x + y) / valuesAndPreds.count()
    print("Mean Squared Error: " + str(MSE))



