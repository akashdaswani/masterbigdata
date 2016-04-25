from geopy.geocoders import Nominatim
from geopy.distance import great_circle

import ConfigParser
import os.path

CONST_SPLIT_CSV = ";"
CONST_SEPARATOR = "|"
CONST_WRITE_MODE = "w"
CONST_POINT = "."
CONST_EMPTY = ""
AIR_STATIONS = []
CONST_ENTER = '\n'

def saveInMemoryAirStationsCoordenates (dirAirStation):
    fileAirStation = open(dirAirStation)
    for line in fileAirStation.readlines():
        lineSplit = line.split(CONST_SEPARATOR)
        stationCode = lineSplit[0]
        stationAddress = lineSplit[1]
        if (stationAddress == "RED"):
            station = [stationCode, 0.0, 0.0];
            AIR_STATIONS.append(station)
        else:
            location = Nominatim().geocode(stationAddress + " Madrid")
            if location:
                station = [stationCode, location.latitude, location.longitude];
                AIR_STATIONS.append(station)
            else:
                print ("LA SIGUIENTE DIRECCION NO TIENE COODENADAS: " + stationAddress)
    fileAirStation.close()


def unionCatalogFiles(dirMeasurementPoint, dirAirStation, dirUnionResult):
    fileUnionResult = open(dirUnionResult, CONST_WRITE_MODE)
    fileMeasurementPoint = open(dirMeasurementPoint)

    for line in fileMeasurementPoint.readlines():
        line = line.split(CONST_SPLIT_CSV)
        id = line[0]
        elementType = line[1]
        codCent = line[2]
        localizacion = line[3]
        latitude = float(line[4].replace(",", "."))
        longitude = float(line[5].replace(",", "."))

        shortestDistance = 0.0
        arrayPosition = 0
        for i in AIR_STATIONS:
            distance = great_circle({latitude, longitude}, {i[1], i[2]}).km
            # print i[0] + "(" + str(i[1]) + "," + str(i[2]) + ") - " + str(distance)
            if (shortestDistance == 0.0):
                shortestDistance = distance
                arrayPosition = i
            else:
                if (shortestDistance > distance):
                    shortestDistance = distance
                    arrayPosition = i

        unionFileLine = id.strip() + CONST_SEPARATOR\
                        +elementType.strip()+CONST_SEPARATOR\
                        +codCent.strip()+CONST_SEPARATOR\
                        +localizacion.strip()+CONST_SEPARATOR\
                        +str(latitude)+CONST_SEPARATOR\
                        +str(longitude)+CONST_SEPARATOR\
                        +arrayPosition[0]
        #print unionFileLine
        fileUnionResult.write(unionFileLine+CONST_ENTER)

    fileMeasurementPoint.close()
    fileUnionResult.close()



if __name__ == "__main__":

    config = ConfigParser.ConfigParser()
    config.read('configuration.cfg')
    filesInDisk = config.getboolean('BatchProperties', 'FilesInDisk')

    if filesInDisk:
        dirMeasurementPoint = config.get('BatchProperties', 'FileTrafficMeasurementPointsLocal')
        dirAirStation = config.get('BatchProperties', 'FileAirStationsLocal')
        dirUnionResult = config.get('BatchProperties', 'FileTrafficMeasurementPointsGeneratedLocal')
    else:
        dirMeasurementPoint = config.get('BatchProperties', 'FileTrafficMeasurementPointsHDFS')
        dirAirStation = config.get('BatchProperties', 'FileAirStationsHDFS')
        dirUnionResult = config.get('BatchProperties', 'FileTrafficMeasurementPointsGeneratedHDFS')

    saveInMemoryAirStationsCoordenates(dirAirStation)
    unionCatalogFiles(dirMeasurementPoint, dirAirStation, dirUnionResult)
