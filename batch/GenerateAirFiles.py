#!/usr/bin/env python
# -*- coding: utf-8 -*-

from os import listdir

import ConfigParser

filesLocaltion = ""
CONST_SEPARATOR = "|"
CONST_STATION_LIMIT = 8
CONST_FEATURES_LIMIT = 2
CONST_MEASUREMENTS_LIMIT = 5
CONST_FIRST_PHASE = 18
CONST_INITIAL = 0
CONST_POINT = "."
CONST_EMPTY = ""
CONST_INITIAL_SECOND_PHASE_WITH_POINT = 2
CONST_FINAL_SECOND_PHASE_WITH_POINT = 3
CONST_INCREASE_SECOND_PHASE_WITH_POINT = 5
CONST_INITIAL_SECOND_PHASE_WITHOUT_POINT = 3
CONST_FINAL_SECOND_PHASE_WITHOUT_POINT = 4
CONST_INCREASE_SECOND_PHASE_WITHOUT_POINT = 6
CONST_TOTAL_LENGTH_LINE = 205
CONST_WRITE_MODE = "w"
CONST_READ_MODE = "r"
CONST_V = "V"
CONST_ENTER = '\n'
CONST_STATION_NAME = 1
CONST_MAGNITUDE_BELOW_LIMIT = 8
CONST_MAGNITUDE_TOP_LIMIT = 10
CONST_MAGNITUDE_NAME = 1
CONST_MAGNITUDE_NOT_FOUND = "N/A"


def completeSecondPhase(lineSeparator, belowLimit, topLimit, lineOriginal, CONST_INITIAL_SECOND_PHASE, CONST_FINAL_SECOND_PHASE, CONST_INCREASE_SECOND_PHASE, lengthLine):

    lineSeparatorSecondPhase = lineSeparator
    belowLimit = belowLimit + CONST_INITIAL_SECOND_PHASE
    topLimit = topLimit + CONST_FINAL_SECOND_PHASE
    counter = 0
    summation = 0
    while topLimit <= lengthLine:
        data = lineOriginal[belowLimit:topLimit]
        if CONST_V in data:
            counter = counter + 1
            iterationString = data[:2]
            iterationInt = int(iterationString)
            summation = summation + iterationInt
        belowLimit = belowLimit + CONST_INCREASE_SECOND_PHASE
        topLimit = topLimit + CONST_INCREASE_SECOND_PHASE

    lineSeparatorSecondPhase = lineSeparatorSecondPhase + CONST_SEPARATOR + str(summation) + CONST_SEPARATOR + str(counter)
    return lineSeparatorSecondPhase

def setLineSeparatorDaily(lineOriginal):
    lengthLine = len(lineOriginal)
    belowLimit = CONST_INITIAL
    topLimit = CONST_STATION_LIMIT
    stationData = lineOriginal[:topLimit]

    lineSeparator = stationData

    belowLimit = topLimit
    topLimit = topLimit+CONST_FEATURES_LIMIT

    while topLimit <= CONST_FIRST_PHASE:
        data = lineOriginal[belowLimit:topLimit]
        print data

        lineSeparator=lineSeparator+CONST_SEPARATOR+data

        belowLimit = topLimit
        topLimit = topLimit + CONST_FEATURES_LIMIT

    if lengthLine == CONST_TOTAL_LENGTH_LINE:

        lineSeparatorSalida = completeSecondPhase(lineSeparator, belowLimit, topLimit, lineOriginal,
                                            CONST_INITIAL_SECOND_PHASE_WITHOUT_POINT,
                                            CONST_FINAL_SECOND_PHASE_WITHOUT_POINT,
                                            CONST_INCREASE_SECOND_PHASE_WITHOUT_POINT, lengthLine)
    else:
        lineSeparatorSalida = completeSecondPhase(lineSeparator, belowLimit, topLimit, lineOriginal,
                                            CONST_INITIAL_SECOND_PHASE_WITH_POINT,
                                            CONST_FINAL_SECOND_PHASE_WITH_POINT,
                                            CONST_INCREASE_SECOND_PHASE_WITH_POINT, lengthLine)
    return lineSeparatorSalida

def stationName(file, searchLine):
    word = searchLine[:CONST_STATION_LIMIT]
    fileCatalog = open(file)
    for line in fileCatalog.readlines():
        if word in line:
            lineSplit = line.split(CONST_SEPARATOR)
            station=lineSplit[CONST_STATION_NAME]
    return station


def magnitudeName(file, searchLine):
    magnitude = CONST_MAGNITUDE_NOT_FOUND
    word = searchLine[CONST_MAGNITUDE_BELOW_LIMIT:CONST_MAGNITUDE_TOP_LIMIT]
    fileMagnitude = open(file)
    for line in fileMagnitude.readlines():
        if word in line:
            lineSplit = line.split(CONST_SEPARATOR)
            magnitude = lineSplit[CONST_MAGNITUDE_NAME]
    return magnitude

def unionFiles(fileAirStations, fileCatalog, fileMagnitude, dirFileOut):
    f = open(dirFileOut, CONST_WRITE_MODE)
    fileAir = open(fileAirStations)
    for line in fileAir.readlines():
        line = line.replace(CONST_POINT,CONST_EMPTY)
        lineSeparator = setLineSeparatorDaily(line)
        station = stationName(fileCatalog, line)
        magnitude = magnitudeName(fileMagnitude, line)
        unionFileLine = magnitude+CONST_SEPARATOR+station+CONST_SEPARATOR+lineSeparator
        print unionFileLine
        f.write(unionFileLine+CONST_ENTER)
    f.close()


if __name__ == "__main__":

    config = ConfigParser.ConfigParser()
    config.read('configuration.cfg')

    filesInDisk = config.getboolean('BatchProperties', 'FilesInDisk')

    if filesInDisk:
        dirFilesAir = config.get('BatchProperties', 'FilesAirLocal')
        dirAirStation = config.get('BatchProperties', 'FileAirStationsLocal')
        dirAirScale = config.get('BatchProperties', 'FileAirScaleLocal')
        dirFilesAirGenerated = config.get('BatchProperties', 'FilesAirGeneratedLocal')
    else:
        dirFilesAir = config.get('BatchProperties', 'FilesAirHDFS')
        dirAirStation = config.get('BatchProperties', 'FileAirStationsHDFS')
        dirAirScale = config.get('BatchProperties', 'FileAirScaleHDFS')
        dirFilesAirGenerated = config.get('BatchProperties', 'FilesAirGeneratedHDFS')

    for fileName in listdir(dirFilesAir):
        if (fileName.endswith("txt")):
            unionFiles(dirFilesAir + fileName, dirAirStation, dirAirScale, dirFilesAirGenerated + fileName)
