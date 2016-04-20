from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext

import ConfigParser

if __name__ == "__main__":
    sc= SparkContext(appName="SparkContext")

    sqlContext = SQLContext(sc)

    config = ConfigParser.ConfigParser()
    config.read('configuration.cfg')

    filesInDisk = config.getboolean('BatchProperties', 'FilesInDisk')

    filesLocaltion = ""

    if filesInDisk:
        filesLocaltion = config.get('BatchProperties', 'FilesLocaltionDisk')
    else:
        filesLocaltion = config.get('BatchProperties', 'FilesLocaltionHDFS')

    df = sqlContext.read.text(filesLocaltion)

    # Displays the content of the DataFrame to stdout
    df.show()

