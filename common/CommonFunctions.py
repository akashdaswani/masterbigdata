import datetime

class CommonFunctions:

    def __init__(self):
        self.data = []

    @staticmethod
    def toWeekday (year, month, day):
        return datetime.date(year, month, day).weekday()
        # if weekday  == 0:
        #    return 1
        # if weekday == 1:
        #    return 10
        #if weekday == 2:
        #    return 100
        #if weekday == 3:
        #    return 1000
        #if weekday == 4:
        #    return 10000
        #if weekday == 5:
        #    return 100000
        #else:
        #    return 1000000

    @staticmethod
    def clasification_intensity (intensity):
        if intensity < 10:
            return 1
        if intensity < 50:
            return 2
        if intensity < 100:
            return 3
        if intensity < 500:
            return 4
        if intensity < 1000:
            return 5
        if intensity < 1500:
            return 6
        else:
            return 7
