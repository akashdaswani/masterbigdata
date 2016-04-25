from os import listdir

import ConfigParser
import os.path

CONST_SPLIT_CSV = ";"
CONST_SPLIT_SPACE = " "
CONST_SEPARATOR = "|"
CONST_WRITE_MODE = "w"
CONST_ENTER = '\n'
TRAFFIC_MEASUREMENT_POINTS = []

#3550|495|50035|(AFOROS)OPORTO E-O(PORTALEGRE-ELVAS)|438599.042006|4471016.47|28079058
def saveInMemoryCatalogMeasurementPoints (dirMeasurementPoint):
    fileMeasurementPoint = open(dirMeasurementPoint)
    for line in fileMeasurementPoint.readlines():
        lineSplit = line.split(CONST_SEPARATOR)
        idTraffic = lineSplit[0]
        codTraffic = lineSplit[2]
        stationCode = lineSplit[6]
        measurementPoint = [idTraffic, codTraffic, stationCode];
        TRAFFIC_MEASUREMENT_POINTS.append(measurementPoint)
    fileMeasurementPoint.close()

def getAirMeasurementPoint(stationCode):
    for measurementPoint in TRAFFIC_MEASUREMENT_POINTS:
        if measurementPoint[0] == stationCode or str('0' + measurementPoint[0]) == stationCode:
            return measurementPoint[2]

    for measurementPoint in TRAFFIC_MEASUREMENT_POINTS:
        if measurementPoint[1] == stationCode or str('0' + measurementPoint[1]) == stationCode:
            return measurementPoint[2]


#identif;fecha;intensidad;ocupacion;carga;tipo;vmed;error;periodo_integracion
#PM20152;12/7/13 7:15;1065;9;48;M;73;N;4
def processTrafficFile (dirTraffic, fileName, dirResult):
    fileAirStation = open(dirTraffic + fileName)
    fileAirStationProcess = open(dirResult + fileName, CONST_WRITE_MODE)
    lines = fileAirStation.readlines()
    for line in lines[0].split('\r'):
        lineSplit = line.split(CONST_SPLIT_CSV)
        header = lineSplit[0]
        if header <> 'identif':
            stationCode = lineSplit[0]
            airMeasurementPoint = getAirMeasurementPoint(stationCode)

            if len(lineSplit) > 1:

                datetime = lineSplit[1]
                dateTimeSplit = datetime.split(CONST_SPLIT_SPACE)
                date = dateTimeSplit[0]
                dateSplit = date.split("/")
                unionFileLine = str(lineSplit[0])+CONST_SEPARATOR\
                                +str(dateSplit[2])+CONST_SEPARATOR\
                                +str(dateSplit[1])+CONST_SEPARATOR\
                                +str(dateSplit[0])+CONST_SEPARATOR\
                                +str(lineSplit[2])+CONST_SEPARATOR\
                                +str(lineSplit[3])+CONST_SEPARATOR\
                                +str(lineSplit[4])+CONST_SEPARATOR\
                                +str(lineSplit[5])+CONST_SEPARATOR\
                                +str(lineSplit[6])+CONST_SEPARATOR\
                                +str(lineSplit[7])+CONST_SEPARATOR\
                                +str(lineSplit[8])+CONST_SEPARATOR\
                                +str(airMeasurementPoint)

                if unionFileLine.endswith(CONST_ENTER):
                    fileAirStationProcess.write(unionFileLine)
                else:
                    fileAirStationProcess.write(unionFileLine + CONST_ENTER)

            else:
                print "NO EXISTE LA FECHA EN LA LINEA " + str(line)

    fileAirStation.close()
    fileAirStationProcess.close()

if __name__ == "__main__":

    config = ConfigParser.ConfigParser()
    config.read('configuration.cfg')
    filesInDisk = config.getboolean('BatchProperties', 'FilesInDisk')

    if filesInDisk:
        dirMeasurementPoint = config.get('BatchProperties', 'FileTrafficMeasurementPointsGeneratedLocal')
        dirTrafficFiles = config.get('BatchProperties', 'FilesTrafficLocal')
        dirTrafficFilesGenerated = config.get('BatchProperties', 'FilesTrafficGeneratedLocal')

    else:
        dirMeasurementPoint = config.get('BatchProperties', 'FileTrafficMeasurementPointsGeneratedHDFS')
        dirTrafficFiles = config.get('BatchProperties', 'FilesTrafficHDFS')
        dirTrafficFilesGenerated = config.get('BatchProperties', 'FilesTrafficGeneratedHDFS')

    saveInMemoryCatalogMeasurementPoints(dirMeasurementPoint)

    for fileName in listdir(dirTrafficFiles):
        if (fileName.endswith("csv")):
            processTrafficFile(dirTrafficFiles, fileName, dirTrafficFilesGenerated)
