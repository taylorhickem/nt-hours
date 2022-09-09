'''time series operations
'''
# -----------------------------------------------------
# Import
# -----------------------------------------------------
import pandas as pd
import datetime as dt
import math


# -----------------------------------------------------
# Module variables
# -----------------------------------------------------
# -----------------------------------------------------
# Setup
# -----------------------------------------------------
# -----------------------------------------------------
# TimeSeriesTable
# -----------------------------------------------------

class TimeSeriesTable():
    data = None
    ts = None
    dtField = ''
    def __init__(self,data,dtField='datetime'):
        if isinstance(data,pd.DataFrame):
            self.data = data
            self.dtField = dtField
            if self.is_timeSeries(data)>0:
                self.ts = self.as_timeSeries(data)
    def is_timeSeries(self,data):
        cond = [0 for x in range(3)]
        cond[0] = len(data)>0
        cond[1] = self.dtField in data.columns
        if (cond[0] and cond[1]):
            cond[2] = isinstance(data.iloc[0][self.dtField],dt.datetime)
        return all(cond)
    def as_timeSeries(self,data):
        ts = data.copy()
        ts = self.addFields_ymwk(ts)
        ts.set_index(self.dtField,inplace=True)
        return ts
    def addFields_ymwk(self,data):
        data['year'] = data.apply(lambda x:x[self.dtField].year,axis=1)
        data['month'] = data.apply(lambda x:x[self.dtField].month,axis=1)
        data['week'] = data.apply(lambda x:self.get_weekNumber(x[self.dtField]),axis=1)
        data['DOW'] = data.apply(lambda x:x[self.dtField].weekday(),axis=1)
        return data
    def get_weekNumber(self,dateValue):
        yearLng = dateValue.year
        yearStartDate = dt.datetime(yearLng,1,1) - dt.timedelta(dt.datetime(yearLng,1,1).weekday())
        weekNumber = math.floor((dateValue-yearStartDate).days/7)+1
        return weekNumber
    def head(self):
        return self.ts.head()

# -----------------------------------------------------
# ****
# -----------------------------------------------------