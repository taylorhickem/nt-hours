''' parsing NowThen records into data model
'''
#-----------------------------------------------------
# Import
#-----------------------------------------------------
import pandas as pd
import datetime as dt

import database as db
from time_series import TimeSeriesTable
from database import CSVDirectory
#-----------------------------------------------------
# Module variables
#-----------------------------------------------------


#constants
HRS_IN_WEEK = 168
HRS_IN_DAY = 24
DELIM = '#'
EVENT_FIELDS = ['timestamp','date','time','activity',
                'duration_hrs','year','month','week','DOW','comment']


#dynamic
directory_path = ''
DIRECTORY_CONFIG = {}
events = None


# custom class objects from other modules
new_records = None


#-----------------------------------------------------
# Setup
#-----------------------------------------------------


def load():
    db.load()
    load_directory()


def load_directory():
    global directory_path, DIRECTORY_CONFIG
    directory_path = db.CONFIG['nowthen_directory']
    DIRECTORY_CONFIG = db.json.load(open(
        directory_path + '\directory.json'
    ))

#-----------------------------------------------------
# Procedures
#-----------------------------------------------------


def update_events():
    '''Update events database sqlite and gsheet with new NowThen records
    '''
    global events
    #01 load csv files from directory
    load_csv()
    has_events = load_events()
    if new_records.has_csv():
        tables = new_records.get_tables()
        #02 create new events from csv
        eventRcds = []
        for t in tables:
            try:
                eventRcd = events_from_csv(tables[t],t)
                if not eventRcd is None:
                    eventRcds.append(eventRcd)
            except:
                pass
        if len(eventRcds)>0:
            new_events = pd.concat(eventRcds)
            has_events = load_events()
            if not has_events:
                events = new_events
                has_events = True
            else:
                #03 append to events db
                events = events.append(new_events)
    if has_events:
        #04 drop duplicates
        events.drop_duplicates(inplace=True)
        #05 push updates to sqlite
        db.update_table(events.reset_index()[EVENT_FIELDS],'event',False)
        #06 push updates to gsheet
        db.post_to_gsheet(events[
            [f for f in EVENT_FIELDS if not f == 'timestamp']],'events','USER_ENTERED')
        #07 flush csv directory
        flush_csv()


def load_csv():
    global new_records
    new_rcd_path = directory_path + DIRECTORY_CONFIG['new_records']
    new_records = CSVDirectory(new_rcd_path)


def flush_csv():
    global new_records
    if new_records.has_files():
        new_records.flush(directory_path)


def load_events():
    global events
    events = db.get_table('event')
    has_events = not events is None
    if has_events:
        events.set_index('timestamp',inplace=True)
    return has_events


def events_from_csv(data,filename=''):
    ''' create events from NowThen data table
    '''
    try:
        #01 convert the dates and create activity label
        std = standardForm(data)
        #02 add year, month, week
        events = TimeSeriesTable(std,dtField='timestamp').ts
    except:
        events = None
    return events


def standardForm(data):
    std = data.copy()
    std['Parent Task'].fillna('',inplace=True)
    std['activity'] = std.apply(lambda x: x['Parent Task'] + DELIM + x['Task Name'], axis=1)
    std['date'] = std['Start Date'].apply(lambda x:
                                  dt.datetime.strptime(x, '%d/%m/%y').date())
    std['time'] = std['Start Time'].apply(lambda x:
                                  dt.datetime.strptime(x, '%H:%M:%S').time())
    std['timestamp'] = std.apply(lambda x:
                                dt.datetime.combine(x['date'], x['time']), axis=1)
    std.rename(columns={'Duration (hours)':'duration_hrs',
                        'Comment':'comment'},inplace=True)
    del std['Start Date'], std['Start Time'], std['End Date'], std['End Time']
    del std['Parent Task'], std['Task Name']
    return std


#-----------------------------------------------------
# Reference code
#-----------------------------------------------------

# class NowThenAgent(object):
#     global HRS_IN_WEEK, HRS_IN_DAY
#     tsTable = None
#     qryData = {}
#     configData = {}
#     config_filenames = {'AIDkey': 'AIDkey.csv', 'roles': 'roles.csv', 'KPI': 'KPI.csv',
#                         'KPIRcds': 'kpi_records\\KPIRcds.csv'}
#     csv_folder = 'C:\\Users\\taylo\\Helva\\06 Quality assurance\\00 QMS database\\01 NowThen data'
#     record_basis = ''
#     period_basis = ''
#     period_week = 0
#     period_month = 0
#     period_startdate = None
#     period_totalHrs = 0
#
#     def __init__(self, csv_folder=None, record_basis='day', period_basis='week'):
#         if not csv_folder is None:
#             self.csv_folder = csv_folder
#         self.record_basis = record_basis
#         self.period_basis = period_basis
#         self.load()
#
#     def load(self):
#         # 01 import config settings
#         self.load_config_files()
#         # 02 import csv monthly files as pd DataFrames
#         csv_files = fso.getFilesInFolder(self.csv_folder)
#         csv_filenames = [f for f in csv_files if 'nowthen_then_' + self.record_basis in f]
#         csv_tables = []
#         for f in csv_filenames:
#             df = pd.read_csv(self.csv_folder + '\\' + f)
#             csv_tables.append(df)
#         # 03 concat all df into single df
#         rawData = pd.concat(csv_tables)
#         stdData = self.standardForm(rawData)
#         self.tsTable = TimeSeriesTable(stdData)
#
#     def load_config_files(self):
#         for f in self.config_filenames:
#             self.configData[f] = pd.read_csv(
#                 self.csv_folder + '\\' + self.config_filenames[f])
#
#     def getAID(self, task, parent):
#         if task[:3].split()[0].isdigit():
#             tId = int(task[:3].split()[0])
#         else:
#             tId = 0
#         if isinstance(parent, str):
#             pId = int(parent[:3].split()[0])
#         else:
#             pId = 0
#
#         if pId == 0:
#             AID = tId * 10000
#         elif tId > 99:
#             AID = tId * 100
#         else:
#             AID = pId * 100 + tId
#         return AID
#
#     def standardForm(self, data):
#         std = data.copy()
#         std['AID'] = std.apply(lambda x:
#                                self.getAID(x['Task Name'], x['Parent Task']), axis=1)
#         std['start_date'] = std.apply(lambda x:
#                                       dt.datetime.strptime(x['Start Date'], '%d/%m/%y'), axis=1)
#         std['start_time'] = std.apply(lambda x:
#                                       dt.datetime.strptime(x['Start Time'], '%H:%M:%S').time(), axis=1)
#         std['datetime'] = std.apply(lambda x:
#                                     dt.datetime.combine(x['start_date'], x['start_time']), axis=1)
#         del std['Start Date']
#         del std['Start Time']
#         return std
#
#     def query_by_AID(self, AID):
#         queryresult = self.tsTable.data.copy()
#         queryresult = queryresult[queryresult['AID'] == AID]
#         return queryresult
#
#     def load_period_hrs_report(self, period_basis='week', relPrd=0, yearPrd=0):
#         nowDate = dt.datetime.now()
#         if period_basis == 'week':
#             weekNumber = self.tsTable.get_weekNumber(nowDate) + relPrd
#             self.period_week = weekNumber
#             rcds = self.tsTable.ts[(self.tsTable.ts.week == weekNumber)
#                                    & (self.tsTable.ts.year == nowDate.year + yearPrd)]
#         elif period_basis == 'month':
#             monthNumber = nowDate.month + relPrd
#             self.period_month = monthNumber
#             rcds = self.tsTable.ts[(self.tsTable.ts.month == monthNumber)
#                                    & (self.tsTable.ts.year == nowDate.year + yearPrd)]
#         else:
#             raise ValueError('unrecognized period: ' + period_basis + ', allowed periods are week and month')
#         self.qryData['periodRcds'] = rcds
#         if len(rcds) > 0:
#             pvt = pd.pivot_table(rcds, index=['AID'], values=['Duration (hours)'], aggfunc='sum')
#             pvt.reset_index(inplace=True)
#             prdHrs = pvt.merge(rcds[['AID', 'Task Name']], on='AID')
#             prdHrs = prdHrs.drop_duplicates().set_index('AID')
#             self.qryData['periodHrs'] = prdHrs
#             self.qryData['newAIDs'] = self.period_newAIDs()
#             self.load_period_settings()
#         else:
#             raise ValueError('no records for selected period')
#
#     def load_period_settings(self):
#         if 'periodHrs' in self.qryData:
#             startdate = self.qryData['periodRcds']['start_date'].min()
#             self.period_startdate = dt.datetime(startdate.year, startdate.month, startdate.day)
#             if self.period_basis == 'week':
#                 self.period_totalHrs = HRS_IN_WEEK
#             elif self.period_basis == 'month':
#                 self.period_totalHrs = calendar.monthrange(self.period_startdate.year,
#                                                            self.period_startdate.month)[1] * HRS_IN_DAY
#
#     def period_update_labels(self):
#         if all([x in self.qryData for x in ['periodHrs', 'periodRcds']]):
#             AIDkey = self.configData['AIDkey']
#             prdRcds = self.qryData['periodRcds']
#             labRcds = prdRcds.merge(AIDkey, on='AID')
#             self.qryData['labPeriodRcds'] = labRcds
#
#             prdHrs = self.qryData['periodHrs']
#             labHrs = prdHrs.reset_index().merge(AIDkey, on='AID')
#             self.qryData['labPeriodHrs'] = labHrs
#
#     def periodHrs_checkSum(self):
#         reportHrs = self.qryData['periodHrs']['Duration (hours)'].sum()
#         return self.period_totalHrs - 0.5 < reportHrs < self.period_totalHrs + 0.5
#
#     def period_getAID(self):
#         report_name = 'periodHrs'
#         AID = None
#         if report_name in self.qryData:
#             AID = self.qryData[report_name].index
#         return AID
#
#     def period_newAIDs(self):
#         newAIDs = []
#         report_name = 'periodHrs'
#         if report_name in self.qryData:
#             report_AID = list(self.period_getAID())
#             AIDkey = list(self.configData['AIDkey']['AID'])
#             newId = [AID for AID in report_AID if not AID in AIDkey]
#             newAIDs = self.qryData['periodHrs'].loc[newId]
#         return newAIDs
#
#     def hasKPIRcds(self):
#         return len(self.configData['KPIRcds'])
#
#     def rcds_parse_date(self, rcds, dateField, formatStr, output='date'):
#         if all([len(rcds) > 0, dateField in rcds.columns]):
#             if output == 'date':
#                 rcds['new_date'] = rcds.apply(lambda x: dt.datetime.strptime(x[dateField], formatStr), axis=1)
#             elif output == 'str':
#                 rcds['new_date'] = rcds.apply(lambda x: dt.datetime.strftime(x[dateField], formatStr), axis=1)
#             else:
#                 raise ValueError('only allowed types are str and date: ' + output + ' was passed')
#             del rcds[dateField]
#             rcds.rename(columns={'new_date': dateField}, inplace=True)
#         return rcds
#
#     def availablehrs_kpis(self):
#         # functions to call prior to calling this procedure
#         # -- load_period_hrs_report()
#         # -- period_update_labels()
#         kpiDate = self.period_startdate
#         year = kpiDate.year
#         weekNumber = self.tsTable.get_weekNumber(kpiDate)
#         kpiValues = {}
#         kpiGroup = 'available hrs'
#         if 'labPeriodHrs' in self.qryData:
#             labHrs = self.qryData['labPeriodHrs']
#             # 01 calculate kpis and store into dictionary
#             # 01.01 overall hrs KPIs
#             groupHrs = labHrs[['group', 'Duration (hours)']].groupby(
#                 'group').sum()['Duration (hours)'] * HRS_IN_WEEK / self.period_totalHrs
#             kpiValues['helva total hrs'] = groupHrs['helva']
#             kpiValues['available hrs'] = HRS_IN_WEEK - groupHrs['basic'] - groupHrs['sleep']
#             kpiValues['sleep'] = groupHrs['sleep'] / 7  # hpd
#             kpiValues['basic'] = groupHrs['basic'] / 7  # hpd
#
#             if 'helva' in groupHrs:
#                 kpiValues['helva pct avail'] = groupHrs['helva'] / kpiValues['available hrs']
#             else:
#                 kpiValues['helva pct avail'] = 0
#             if 'husband' in groupHrs:
#                 kpiValues['husband pct avail'] = groupHrs['husband'] / kpiValues['available hrs']
#             else:
#                 kpiValues['husband pct avail'] = 0
#             if 'finances' in groupHrs:
#                 kpiValues['finances pct avail'] = groupHrs['finances'] / kpiValues['available hrs']
#             else:
#                 kpiValues['finances pct avail'] = 0
#             if 'custodian' in groupHrs:
#                 kpiValues['custodian pct avail'] = groupHrs['custodian'] / kpiValues['available hrs']
#             else:
#                 kpiValues['custodian pct avail'] = 0
#             if 'slack' in groupHrs:
#                 kpiValues['slack pct avail'] = groupHrs['slack'] / kpiValues['available hrs']
#             else:
#                 kpiValues['slack pct avail'] = 0
#             # 01.02 role and activity specific KPIs
#             labPeriodRcds = self.qryData['labPeriodRcds']
#             activityHrs = pd.pivot_table(labPeriodRcds, index=['group', 'role', 'label'],
#                                          values=['Duration (hours)'], aggfunc='sum')
#             kpiValues['OnTrack daily routine'] = activityHrs.loc[
#                                                      ('helva', '4 Executive Leadership', 'update daily plan')][
#                                                      0] * HRS_IN_DAY / self.period_totalHrs
#         return kpiValues
#
#     def availablehrs_update_kpi(self):
#         kpiDate = self.period_startdate
#         year = kpiDate.year
#         weekNumber = self.tsTable.get_weekNumber(kpiDate)
#         kpiGroup = 'available hrs'
#         kpiValues = self.availablehrs_kpis()
#         kpiDash = self.configData['KPI']
#         KPIRcds = self.rcds_parse_date(self.configData['KPIRcds'], 'date', '%d/%m/%y', output='date')
#         if kpiValues != {}:
#             # 02 update kpi dashboard
#             for kpi in kpiValues:
#                 kpiDash.loc[(kpiDash.label == kpi) & (kpiDash.group == kpiGroup),
#                             ['last value']] = kpiValues[kpi]
#                 kpiDash.loc[(kpiDash.label == kpi) & (kpiDash.group == kpiGroup),
#                             ['date']] = kpiDate
#             kpiDash.to_csv(self.csv_folder + '\\' + self.config_filenames['KPI'], index=False)
#             # 03 update kpi records
#             # 03.01 check if kpi data rows exist
#             hasRcds = len(KPIRcds) > 0
#             if hasRcds:
#                 hasRcds = len(KPIRcds.loc[(KPIRcds.group == kpiGroup) & (KPIRcds.date == kpiDate)]) > 0
#                 if hasRcds:
#                     # 03.02.01 update
#                     for kpi in kpiValues:
#                         # if record exists for the specific kpi
#                         if len(KPIRcds.loc[(KPIRcds.label == kpi) & (KPIRcds.group == kpiGroup)
#                                            & (KPIRcds.date == kpiDate)]) > 0:
#                             # update the record
#                             KPIRcds.loc[(KPIRcds.label == kpi) & (KPIRcds.group == kpiGroup)
#                                         & (KPIRcds.date == kpiDate), ['value']] = kpiValues[kpi]
#                         else:
#                             # append new row
#                             rcds = pd.DataFrame({'label': [kpi], 'group': [kpiGroup], 'value': [kpiValues[kpi]],
#                                                  'date': [kpiDate], 'year': [year], 'week': [weekNumber]},
#                                                 columns=['label', 'group', 'value', 'date', 'year', 'week'])
#                             KPIRcds = KPIRcds.append(rcds)
#                 else:
#                     # append new rows
#                     rcds = pd.DataFrame({'label': [k for k in kpiValues],
#                                          'group': [kpiGroup for k in kpiValues], 'date': [kpiDate for k in kpiValues],
#                                          'value': [kpiValues[k] for k in kpiValues],
#                                          'year': [year for k in kpiValues], 'week': [weekNumber for k in kpiValues]},
#                                         columns=['label', 'group', 'value', 'date', 'year', 'week'])
#                     KPIRcds = KPIRcds.append(rcds)
#             else:
#                 # create new df
#                 KPIRcds = pd.DataFrame({'label': [k for k in kpiValues],
#                                         'group': [kpiGroup for k in kpiValues], 'date': [kpiDate for k in kpiValues],
#                                         'value': [kpiValues[k] for k in kpiValues],
#                                         'year': [year for k in kpiValues], 'week': [weekNumber for k in kpiValues]},
#                                        columns=['label', 'group', 'value', 'date', 'year', 'week'])
#             KPIRcds = self.rcds_parse_date(KPIRcds, 'date', '%d/%m/%y', output='str')
#             KPIRcds.to_csv(self.csv_folder + '\\' + self.config_filenames['KPIRcds'], index=False)
#
#     def helvahrs_create_report(self):
#         if 'labPeriodRcds' in self.qryData:
#             labPeriodRcds = self.qryData['labPeriodRcds']
#             # create the helva hrs summary for all roles
#             helvaRcds = labPeriodRcds[labPeriodRcds.group == 'helva']
#             helvaHrs = pd.pivot_table(helvaRcds, index=['role', 'label'], values=['Duration (hours)'], aggfunc='sum')
#             self.qryData['helvaHrs'] = helvaHrs
#             # create detailed session reports for each role
#             roles = list(helvaRcds.role.unique())
#             roleHrs = {}
#             for role in roles:
#                 roleRcds = helvaRcds[helvaRcds.role == role]
#                 roleHrs[role] = pd.pivot_table(roleRcds, index=['label', 'Comment'], values=['Duration (hours)'],
#                                                aggfunc='sum')
#             self.qryData['roleHrs'] = roleHrs
#
#     def helvahrs_role_report(self, role, normalize=True):
#         report = None
#         if 'roleHrs' in self.qryData:
#             roleHrs = self.qryData['roleHrs']
#             role_totalHrs = roleHrs[role]['Duration (hours)'].sum()
#             print(role + ': total hrs ' '%.1f' % role_totalHrs)
#             normF = 1
#             if role_totalHrs > 0:
#                 if normalize:
#                     normF = 100 / role_totalHrs
#                 report = roleHrs[role] * normF
#         return report