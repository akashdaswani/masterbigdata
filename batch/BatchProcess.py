from pyspark import SparkContext
from pyspark.sql import SQLContext

if __name__ == "__main__":
    sc= SparkContext(appName="PythonSparkLocal")

    sqlContext = SQLContext(sc)

    df = sqlContext.read.text("/Users/akash/PycharmProjects/masterbigdata/datasets/batch/calidadaire/datos13.txt")

    # Displays the content of the DataFrame to stdout
    df.show()