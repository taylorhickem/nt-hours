''' parsing Toggl records into data model
'''
#-----------------------------------------------------
# Import
#-----------------------------------------------------
import pandas as pd
import datetime as dt
from nthours import database as db
from nthours.time_series import TimeSeriesTable
from nthours import toggl
#-----------------------------------------------------
# Module variables
#-----------------------------------------------------


#constants
WINDOW_YEARS = 1
HRS_IN_WEEK = 168
HRS_IN_DAY = 24
DELIM = '#'
EVENT_FIELDS = ['timestamp', 'date', 'time', 'activity',
                'duration_hrs', 'year', 'month', 'week', 'DOW', 'comment']
MIME_TYPE_CSV = 'text/csv'

#dynamic
events = None


#-----------------------------------------------------
# Setup
#-----------------------------------------------------


def load():
    db.load()

#-----------------------------------------------------
# Procedures
#-----------------------------------------------------


def update_events(report_date=None):
    '''Update events database sqlite and gsheet with new Toggl records
    '''
    global events

    #01 load new events
    new_events = events_std_format(None, '', report_date)
    has_new_events = len(new_events) > 0

    #02 load db events
    has_events = load_events()

    if has_new_events:
        if has_events:
            events = events.append(new_events)
        else:
            events = new_events.copy()
            has_events = True

        # 04 drop duplicates and sort
        events = events[~events.index.duplicated(keep='first')]
        events.sort_index(inplace=True)

        # 05 push updates to sqlite
        db.update_table(events.reset_index()[EVENT_FIELDS],
                        'event',
                        False)

    if has_events:
        # 06 format fields for gsheet
        rngcode = 'events'

        # 06.01 date and time to str
        date_format = db.GSHEET_CONFIG[rngcode]['date_format']
        time_format = db.GSHEET_CONFIG[rngcode]['time_format']

        def event_time_convert(time_value):
            time_str = ''
            if isinstance(time_value, str):
                time_str = time_value[:-3]
            else:
                time_str = time_value.strftime(time_format)
            return time_str

        events['date'] = events['date'].apply(lambda x: dt.datetime.strftime(x, date_format))
        events['time'] = events['time'].apply(lambda x: event_time_convert(x))

        # 06.02 fill empty str for blank comment fields
        events['comment'].fillna('', inplace=True)

        #07 push recent events to gsheet
        min_year = events['year'].max() - WINDOW_YEARS + 1
        recent = events[events['year'] >= min_year].copy()
        db.post_to_gsheet(recent[
            [f for f in EVENT_FIELDS if not f == 'timestamp']], rngcode, 'USER_ENTERED')


def load_events():
    global events
    events = db.get_table('event')
    has_events = not events is None
    if has_events:
        events.set_index('timestamp', inplace=True)
    return has_events


def events_std_format(data, filename='', report_date=None):
    ''' create events from Toggl data table
    '''
    try:
        #01 convert the dates and create activity label
        if not report_date:
            report_date = dt.datetime.today().strftime(toggl.TOGGL_DATE_FORMAT)
        std = toggl.std_events_from_api(report_date)
        # std = toggl.standard_form(data)  # deprecated
        # std = nt_standardForm(data)      # deprecated, NowThen method

        #02 add year, month, week
        events = TimeSeriesTable(std, dtField='timestamp').ts

    except:
        events = None

    return events
