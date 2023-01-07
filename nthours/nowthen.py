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
GDRIVE_CONFIG = {}
GDRIVE_FOLDER_IDS = {}

#dynamic
events = None
new_records_asbytes = []


#-----------------------------------------------------
# Setup
#-----------------------------------------------------


def load():
    db.load()
    load_gdrive_config()


def load_gdrive_config():
    global GDRIVE_CONFIG, GDRIVE_FOLDER_IDS
    GDRIVE_CONFIG = db.json.load(open('gdrive_config.json'))
    GDRIVE_FOLDER_IDS = GDRIVE_CONFIG['folder_ids']

#-----------------------------------------------------
# Procedures
#-----------------------------------------------------


def update_events():
    '''Update events database sqlite and gsheet with new Toggl records
    '''
    global events

    #01 load csv files
    has_new_events = False
    gdrive_load_csv()
    if len(new_records_asbytes) > 0:
        tables = get_tables_from_brecords()

        #02 create new events from csv
        eventRcds = []
        for t in tables:
            try:
                eventRcd = events_from_csv(tables[t], t)
                if not eventRcd is None:
                    eventRcds.append(eventRcd)
            except:
                pass
        has_new_events = (len(eventRcds) > 0)
    has_events = load_events()

    if has_new_events:
        new_events = pd.concat(eventRcds)

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

        #08 flush csv directory
        gdrive_flush_csv()


def gdrive_load_csv():
    #store results in 'new_records_as_bytes'
    file_references = db.gdrive.get_files_in_folder(
        folder_name='',
        folder_id=GDRIVE_FOLDER_IDS['new_records'],
        include_subfolders=False,
        mime_type=MIME_TYPE_CSV
    )
    if len(file_references) > 0:
        for f in file_references:
            bdata = db.gdrive.download_file(f['id'])
            rcd = {
                'id': f['id'],
                'filename': f['name'],
                'bdata': bdata
            }
            new_records_asbytes.append(rcd)


def gdrive_flush_csv():
    if len(new_records_asbytes) > 0:
        file_ids = [f['id'] for f in new_records_asbytes]
        db.gdrive.move_files_to_folder(
            file_ids,
            destination_id=GDRIVE_FOLDER_IDS['root'],
            source_id=GDRIVE_FOLDER_IDS['new_records']
        )


def get_tables_from_brecords():
    events_filename = 'events.csv'
    tables = {}
    if len(new_records_asbytes) > 0:
        for rcd in new_records_asbytes:
            #write byte file to csv
            filename = rcd['filename']
            bdata = rcd['bdata']
            with open(events_filename, "wb") as bevents:
                bevents.write(bdata)
            bevents.close()

            #import csv to pandas DataFrame
            df = pd.read_csv(events_filename)
            db.os.remove(events_filename)
            tables[filename] = df

    return tables


def load_events():
    global events
    events = db.get_table('event')
    has_events = not events is None
    if has_events:
        events.set_index('timestamp', inplace=True)
    return has_events


def events_from_csv(data, filename=''):
    ''' create events from Toggl data table
    '''
    try:
        #01 convert the dates and create activity label
        std = toggl.standard_form(data)
        # std = nt_standardForm(data)      # deprecated, NowThen method

        #02 add year, month, week
        events = TimeSeriesTable(std, dtField='timestamp').ts

    except:
        events = None

    return events

# deprecated method
#def nt_standardForm(data):
#    std = data.copy()
#    std['Parent Task'].fillna('', inplace=True)
#    std['activity'] = std.apply(lambda x: x['Parent Task'] + DELIM + x['Task Name'], axis=1)
#    std['date'] = std['Start Date'].apply(lambda x:
#                                  dt.datetime.strptime(x, '%d/%m/%y').date())
#    std['time'] = std['Start Time'].apply(lambda x:
#                                  dt.datetime.strptime(x, '%H:%M:%S').time())
#    std['timestamp'] = std.apply(lambda x:
#                                dt.datetime.combine(x['date'], x['time']), axis=1)
#    std.rename(columns={'Duration (hours)': 'duration_hrs',
#                        'Comment': 'comment'}, inplace=True)
#    del std['Start Date'], std['Start Time'], std['End Date'], std['End Time']
#    del std['Parent Task'], std['Task Name']
#    return std

# deprecated method
#def record_date_from_filename(rcd_filename):
#    rcd_label = 'nowthen_then_day_'
#    rcd_date_str = rcd_filename.replace(rcd_label, '')[:-4]
#    rcd_date = dt.datetime.strptime(rcd_date_str, '%Y-%m-%d').date()
#    return rcd_date

#-----------------------------------------------------
# END
#-----------------------------------------------------