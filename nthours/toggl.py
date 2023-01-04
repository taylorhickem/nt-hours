"""this module integrates toggl track events https://github.com/toggl/toggl_api_docs/
"""
import math
import pandas as pd
import datetime as dt

HRS_PER_DAY = 24
DAY_HRS_TOLERANCE = 2
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


def split_overlap_events(events):
    # 01 split the events into overlap and non-overlap
    ovlp = events[events['Start date'] != events['End date']]
    non_ovlp = events[events['Start date'] == events['End date']]

    if len(ovlp) > 0:
        # 02 create end of day events from ovlp
        eod = end_of_day_from_overlap(ovlp)

        # 03 append eod events to the non_ovlp
        non_ovlp = non_ovlp.append(eod)

    return non_ovlp