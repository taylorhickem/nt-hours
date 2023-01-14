"""this module integrates toggl track events https://github.com/toggl/toggl_api_docs/
"""
import json
import math
import pandas as pd
import datetime as dt
import requests
import pytz

api = None

tz_local = None
tz_UTC = pytz.timezone('UTC')
TOGGL_API_URL = 'https://api.track.toggl.com/api/v9'
TOGGL_URL = 'https://track.toggl.com'
TOGGL_ACCOUNT = '1235915'
DETAILED_REPORT_URL = TOGGL_URL + '/reports/detailed/' + TOGGL_ACCOUNT + '/from/{date_from}/to/{date_to}'
HRS_PER_DAY = 24
SEC_PER_HOUR = 3600
DAY_HRS_TOLERANCE = 2
TOGGL_TIMESTAMP_FORMAT = '%Y-%m-%dT%H:%M:%S+00:00'
TOGGL_STOP_FORMAT = '%Y-%m-%dT%H:%M:%SZ'
TOGGL_DATE_FORMAT = '%Y-%m-%d'
TOGGL_TIME_FORMAT = '%H:%M:%S'
ACTIVITY_DELIM = '#'
TAG_DELIM = ' - '
STD_FIELDS = [
    'timestamp',
    'date',
    'time',
    'duration_hrs',
    'activity',
    'comment'
]

def standard_form(data):
    """ converts toggl records from csv to the standard events format
    fields = [timestamp, date, time (start time), duration_hrs, activity, comment]
    """
    std = data.copy()

    # 1. Create `activity` = `Client`#`Project` same as NT script
    std['Client'].fillna('', inplace=True)
    std['Project'].fillna('', inplace=True)
    std['activity'] = std.apply(lambda x: x['Client'] + ACTIVITY_DELIM + x['Project'], axis=1)

    # 2. Add the `Tags` as a hyphen delimited prefix to the description `comment` = `Tag` - `Description`
    std['Tags'].fillna('', inplace=True)
    std['Description'].fillna('', inplace=True)
    std['comment'] = std.apply(
        lambda x: x['Tags'] + TAG_DELIM + x['Description'] if len(x['Tags']) > 0 else x['Description'], axis=1)

    # 3. Split events that overlap between two days into two events ending and beginning at midnight to enforce 24 hours in a day
    std = split_overlap_events(std)

    # 4. Parse the `Start date`, `Start time` strings into datetime objects
    std['date'] = std['Start date'].apply(lambda x: dt.datetime.strptime(x, TOGGL_DATE_FORMAT).date())
    std['time'] = std['Start time'].apply(lambda x: dt.datetime.strptime(x, TOGGL_TIME_FORMAT).time())

    # 5. Create `timestamp` from `date` and `time` same as the NT script
    std['timestamp'] = std.apply(lambda x: dt.datetime.combine(x['date'], x['time']), axis=1)

    # 6. Calculate duration_hrs = D[:2] + (D[3:5] + D[6:8]/60])/60 from the string Duration
    std['duration_hrs'] = std['Duration'].apply(lambda x: hours_from_timestamp(x))

    std = std[STD_FIELDS]

    # 7. drop partial dates
    hbd = pd.pivot_table(std, index='date', values='duration_hrs', aggfunc='sum').reset_index()
    full_dates = hbd[hbd.duration_hrs > (HRS_PER_DAY - DAY_HRS_TOLERANCE)]['date'].to_list()
    keep = std[std['date'].isin(full_dates)]

    return keep


def hours_from_timestamp(ts_str):
    # H:M:S
    hrs = int(ts_str[:2])+(int(ts_str[3:5])+int(ts_str[6:8])/60)/60
    return hrs


def timestamp_from_hours(hrs):
    hms = []
    hms.append(math.floor(hrs))                     # hour
    hms.append(math.floor((hrs-hms[0])*60))         # min
    hms.append(round(((hrs-hms[0])*60-hms[1])*60))  # sec
    timestamp = ':'.join(['%.2d' % x for x in hms])
    return timestamp


def end_of_day_from_overlap(events):
    def duration_same_day(start_time):
        start_hrs = hours_from_timestamp(start_time)
        same_hrs = 24 - start_hrs
        same_dur = timestamp_from_hours(same_hrs)
        return same_dur

    def duration_next_day(start_time, ovlp_dur):
        ovlp_hrs = hours_from_timestamp(ovlp_dur)
        start_hrs = hours_from_timestamp(start_time)
        same_hrs = 24 - start_hrs
        next_hrs = ovlp_hrs - same_hrs
        next_dur = timestamp_from_hours(next_hrs)
        return next_dur

    d0 = events.copy()
    d1 = events.copy()

    d0['Duration'] = d0['Start time'].apply(lambda x: duration_same_day(x))
    d1['Duration'] = d1.apply(lambda x: duration_next_day(x['Start time'], x['Duration']), axis=1)
    d1['Start date'] = d1['End date']
    d1['Start time'] = '00:00:00'

    eod = pd.concat([d0, d1], axis=0)
    return eod


def split_overlap_events(events, method='api_entries'):
    # 01 split the events into overlap and non-overlap
    if method == 'api_entries':
        ovlp = events[events['start_date'] != events['stop_date']]
        non_ovlp = events[events['start_date'] == events['stop_date']]

    elif method == 'export_detailed':
        ovlp = events[events['Start date'] != events['End date']]
        non_ovlp = events[events['Start date'] == events['End date']]

    else:
        raise ValueError('method: %s, is not recognized' % method)

    if len(ovlp) > 0:
        # 02 create end of day events from ovlp
        if method == 'api_entries':
            eod = eod_from_ovlp_ts_method(ovlp)

        elif method == 'export_detailed':
            eod = end_of_day_from_overlap(ovlp)

        else:
            raise ValueError('method: %s, is not recognized' % method)

        # 03 append eod events to the non_ovlp
        non_ovlp = non_ovlp.append(eod)

    return non_ovlp


class TogglAPI(object):
    API_TOKEN = ''
    session = None
    auth = None

    def __init__(self):
        self.login()

    def login(self):
        with open('api_token') as f:
            self.API_TOKEN = f.read()
        f.close()

        self.session = requests.Session()
        self.session.auth = (self.API_TOKEN, 'api_token')
        self.auth, status_code = self.get('/me')

    def get(self, route):
        content = {}
        status_code = 400
        try:
            response = self.session.get(TOGGL_API_URL + route)
            status_code = response.status_code
            if status_code == 200:
                content = json.loads(response.content.decode('utf-8'))
        except:
            print('something went wrong...')
        return content, status_code

    def time_entries(self, start_date, end_date):
        route_generic = '/me/time_entries?start_date={start_date}&end_date={end_date}'
        route = route_generic.format(
            start_date=start_date,
            end_date=end_date
        )
        entries, status_code = self.get(route)
        return entries

    def workspace_projects(self):
        route_generic = '/workspaces/{workspace_id}/projects'
        route = route_generic.format(
            workspace_id=self.auth['default_workspace_id']
        )
        projects, status_code = self.get(route)
        return projects

    def workspace_clients(self):
        route_generic = '/workspaces/{workspace_id}/clients'
        route = route_generic.format(
            workspace_id=self.auth['default_workspace_id']
        )
        client, status_code = self.get(route)
        return client


def api_login():
    global api
    if not api:
        api = TogglAPI()


def api_logout():
    global api
    if api:
        api = None


def std_events_from_api(report_date):
    global tz_local
    date_from = (dt.datetime.strptime(report_date, TOGGL_DATE_FORMAT)
                    - dt.timedelta(days=2)
    ).strftime(TOGGL_DATE_FORMAT)

    api_login()

    tz_local = pytz.timezone(api.auth['timezone'])
    entries = api.time_entries(date_from, report_date)
    projects = api.workspace_projects()
    clients = api.workspace_clients()
    events = std_events_from_entries(entries, projects, clients)

    api_logout()

    return events


def std_events_from_entries(entries, projects, clients):
    std = pd.DataFrame.from_records(entries)

    # 01 activity label = Client#Project
    activity_labels = []
    for p in projects:
        rcd = {}
        rcd['id'] = p['id']
        task = p['name']
        client = [c['name'] for c in clients if c['id'] == p['client_id']][0]
        rcd['activity'] = client + ACTIVITY_DELIM + task
        activity_labels.append(rcd)

    activities = pd.DataFrame.from_records(activity_labels)
    std = std.merge(activities, left_on='project_id', right_on='id', how='left')

    # 02 comment = tag - description
    std['description'].fillna('', inplace=True)
    std['comment'] = std.apply(lambda x: x['tags'][0] + TAG_DELIM + x['description']
        if x['tags'] else x['description'], axis=1)

    # 03 convert start time from utc string to sgp timestamp
    std['timestamp'] = std['start'].apply(lambda x: utc_str_to_local_datetime(x, 'start'))
    std['start_date'] = std['timestamp'].apply(lambda x: x.date())
    std['stop_date'] = std['stop'].apply(lambda x: utc_str_to_local_datetime(x, 'stop').date())

    # 04 Split events that overlap between two days into two events ending and beginning at midnight
    # to enforce 24 hours in a day
    std = split_overlap_events(std, method='api_entries')

    # 05 add start date and start time
    std['date'] = std['timestamp'].apply(lambda x: x.date())
    std['time'] = std['timestamp'].apply(lambda x: x.time())

    # 06 convert duration from seconds to hours
    std['duration_hrs'] = std['duration'] / SEC_PER_HOUR

    std = std[STD_FIELDS]

    # 7. drop partial dates
    hbd = pd.pivot_table(std, index='date', values='duration_hrs', aggfunc='sum').reset_index()
    full_dates = hbd[hbd.duration_hrs > (HRS_PER_DAY - DAY_HRS_TOLERANCE)]['date'].to_list()
    keep = std[std['date'].isin(full_dates)]

    return keep


def utc_str_to_local_datetime(utc_str, format_code):
    if format_code == 'start':
        datetime_format = TOGGL_TIMESTAMP_FORMAT
    elif format_code == 'stop':
        datetime_format = TOGGL_STOP_FORMAT
    else:
        raise ValueError('unrecognized format code %s' % format_code)

    utc_timestamp = dt.datetime.strptime(utc_str, datetime_format)
    tza = tz_UTC.localize(utc_timestamp)
    local_timestamp = tza.astimezone(tz_local).replace(tzinfo=None)
    return local_timestamp


def eod_from_ovlp_ts_method(events):
    def at_midnight(timestamp):
        midnight = timestamp.replace(
            hour=0,
            minute=0,
            second=0,
            microsecond=0
        )
        return midnight

    def seconds_from_timestamp(timestamp):
        seconds = (timestamp - at_midnight(timestamp)).seconds
        return seconds

    def next_day_midnight(timestamp):
        next_day = at_midnight(timestamp + dt.timedelta(days=1))
        return next_day

    def duration_same_day(start_time):
        start_sec = seconds_from_timestamp(start_time)
        same_sec = HRS_PER_DAY*SEC_PER_HOUR - start_sec
        return same_sec

    def duration_next_day(start_time, ovlp_sec):
        start_sec = seconds_from_timestamp(start_time)
        same_sec = HRS_PER_DAY*SEC_PER_HOUR - start_sec
        next_sec = ovlp_sec - same_sec
        return next_sec

    d0 = events.copy()
    d1 = events.copy()

    d0['duration'] = d0['timestamp'].apply(lambda x: duration_same_day(x))
    d1['duration'] = d1.apply(lambda x: duration_next_day(x['timestamp'], x['duration']), axis=1)
    d1['timestamp'] = d1['timestamp'].apply(lambda x: next_day_midnight(x))

    eod = pd.concat([d0, d1], axis=0)
    return eod
