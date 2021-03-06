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
CONST_INITIAL_SECOND_PHASE_WITH_POINT = 1
CONST_FINAL_SECOND_PHASE_WITH_POINT = 3
CONST_INCREASE_SECOND_PHASE_WITH_POINT = 5
CONST_INITIAL_SECOND_PHASE_WITHOUT_POINT = 2
CONST_FINAL_SECOND_PHASE_WITHOUT_POINT = 4
CONST_INCREASE_SECOND_PHASE_WITHOUT_POINT = 6
CONST_TOTAL_LENGTH_LINE = 205
CONST_WRITE_MODE = "w"
CONST_READ_MODE = "r"
CONST_V = "V"
CONST_N = "N"
CONST_ENTER = '\n'
CONST_STATION_NAME = 1
CONST_MAGNITUDE_BELOW_LIMIT = 8
CONST_MAGNITUDE_TOP_LIMIT = 10
CONST_MAGNITUDE_NAME = 1
CONST_MAGNITUDE_NOT_FOUND = "N/A"
CONST_CERO = 0
CONST_UNO = 1
CONST_DOS = 2
CONST_TRES = 3

def completeSecondPhase(belowLimit, topLimit, lineOriginal, CONST_INITIAL_SECOND_PHASE, CONST_FINAL_SECOND_PHASE, CONST_INCREASE_SECOND_PHASE, lengthLine):

    lineSeparatorSecondPhase = CONST_EMPTY
    belowLimit = belowLimit + CONST_INITIAL_SECOND_PHASE
    topLimit = topLimit + CONST_FINAL_SECOND_PHASE
    counter = CONST_CERO
    line40 = lineOriginal[:30]
    if "N" in line40:
        error = CONST_UNO
    while topLimit <= lengthLine:
        data = lineOriginal[belowLimit:topLimit]
        if CONST_V in data:
            counter = counter + CONST_UNO
            iterationString = data[:CONST_TRES]
            lineSeparatorSecondPhase = lineSeparatorSecondPhase + CONST_SEPARATOR + iterationString
        else:
            if CONST_N not in data:
                belowLimit = belowLimit+CONST_UNO
                topLimit = topLimit+CONST_UNO
                data= lineOriginal[belowLimit:topLimit]
                if CONST_V in data:
                    counter = counter + CONST_UNO
                    iterationString = data[:CONST_TRES]
                    lineSeparatorSecondPhase = lineSeparatorSecondPhase + CONST_SEPARATOR + iterationString
        belowLimit = belowLimit + CONST_INCREASE_SECOND_PHASE
        topLimit = topLimit + CONST_INCREASE_SECOND_PHASE

    lineSeparatorSecondPhase = str(counter) + lineSeparatorSecondPhase

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

        lineSeparatorSalida = completeSecondPhase(belowLimit, topLimit, lineOriginal,
                                            CONST_INITIAL_SECOND_PHASE_WITHOUT_POINT,
                                            CONST_FINAL_SECOND_PHASE_WITHOUT_POINT,
                                            CONST_INCREASE_SECOND_PHASE_WITHOUT_POINT, lengthLine)
    else:
        lineSeparatorSalida = completeSecondPhase(belowLimit, topLimit, lineOriginal,
                                            CONST_INITIAL_SECOND_PHASE_WITH_POINT,
                                            CONST_FINAL_SECOND_PHASE_WITH_POINT,
                                            CONST_INCREASE_SECOND_PHASE_WITH_POINT, lengthLine)

    lineSeparatorSalida = lineSeparator + CONST_SEPARATOR + lineSeparatorSalida

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
    fileAir = open(fileAirStations)
    f = open(dirFileOut, CONST_WRITE_MODE)
    for line in fileAir.readlines():
        line = line.replace(CONST_POINT,CONST_EMPTY)
        lineSeparator = setLineSeparatorDaily(line)
        station = stationName(fileCatalog, line)
        magnitude = magnitudeName(fileMagnitude, line)
        unionFileLine = magnitude+CONST_SEPARATOR+station+CONST_SEPARATOR+lineSeparator
        lineSeparatorSplit = unionFileLine.split(CONST_SEPARATOR)
        lineSeparatorBase = CONST_EMPTY
        counterBase = CONST_CERO
        for part in lineSeparatorSplit:
            if counterBase <= 7:
                if lineSeparatorBase == CONST_EMPTY:
                    lineSeparatorBase = part
                else:
                    lineSeparatorBase = lineSeparatorBase + CONST_SEPARATOR + part
            counterBase = counterBase + CONST_UNO
        daysInMonth = int(lineSeparatorSplit[8])
        counter = CONST_UNO
        firstStep = 9

        while counter <= daysInMonth:
            monthDay = str(counter)
            if len(monthDay) == 1:
                monthDay = str(CONST_CERO) + monthDay
            lineSeparatorFile = lineSeparatorBase + CONST_SEPARATOR + monthDay + CONST_SEPARATOR + lineSeparatorSplit[firstStep]
            firstStep = firstStep + CONST_UNO
            counter = counter + CONST_UNO
            f.write(lineSeparatorFile + CONST_ENTER)

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