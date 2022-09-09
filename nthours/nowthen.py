''' parsing NowThen records into data model
'''
#-----------------------------------------------------
# Import
#-----------------------------------------------------
import pandas as pd
import datetime as dt
from nthours import database as db
from nthours.time_series import TimeSeriesTable
#from database import CSVDirectory   #deprecated, see note 01
#-----------------------------------------------------
# Module variables
#-----------------------------------------------------


#constants
WINDOW_YEARS = 2
HRS_IN_WEEK = 168
HRS_IN_DAY = 24
DELIM = '#'
EVENT_FIELDS = ['timestamp', 'date', 'time', 'activity',
                'duration_hrs', 'year', 'month', 'week', 'DOW', 'comment']
MIME_TYPE_CSV = 'text/csv'
GDRIVE_CONFIG = {}      # see note 01
GDRIVE_FOLDER_IDS = {}  # see note 01

#dynamic
#directory_path = ''    #deprecated, see note 01
#DIRECTORY_CONFIG = {}  #deprecated, see note 01
events = None
new_records_asbytes = []    # see note 01


# custom class objects from other modules
#new_records = None  #deprecated, see note 01


#-----------------------------------------------------
# Setup
#-----------------------------------------------------


def load():
    db.load()
    load_gdrive_config()
    #load_directory()   #deprecated, see note 01


def load_gdrive_config():
    global GDRIVE_CONFIG, GDRIVE_FOLDER_IDS
    GDRIVE_CONFIG = db.json.load(open('gdrive_config.json'))
    GDRIVE_FOLDER_IDS = GDRIVE_CONFIG['folder_ids']


def load_directory():   #deprecated, see note 01
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
    #01 load csv files
    gdrive_load_csv()
    #local_load_csv()   #deprecated, see note 01
    has_events = load_events()
    if len(new_records_asbytes) > 0:
    #if new_records.has_csv():  #deprecated, see note 01
        tables = get_tables_from_brecords()
        #tables = new_records.get_tables()  #deprecated, see note 01

        #02 create new events from csv
        eventRcds = []
        for t in tables:
            try:
                eventRcd = events_from_csv(tables[t], t)
                if not eventRcd is None:
                    eventRcds.append(eventRcd)
            except:
                pass
        if len(eventRcds) > 0:
            new_events = pd.concat(eventRcds)
            has_events = load_events()
            if not has_events:
                events = new_events
                has_events = True
            else:
                # 03 append to events db
                events = events.append(new_events)
    if has_events:

        # 04 drop duplicates and sort
        events.drop_duplicates(inplace=True)
        events.sort_index(inplace=True)

        # 05 push updates to sqlite
        db.update_table(events.reset_index()[EVENT_FIELDS],
                        'event',
                        False)

        # 06 format fields for gsheet
        rngcode = 'events'

        # 06.01 date and time to str
        date_format = db.GSHEET_CONFIG[rngcode]['date_format']
        time_format = db.GSHEET_CONFIG[rngcode]['time_format']
        events['date'] = events['date'].apply(lambda x: dt.datetime.strftime(x, date_format))
        events['time'] = events['time'].apply(lambda x: x.strftime(time_format))

        # 06.02 fill empty str for blank comment fields
        events['comment'].fillna('', inplace=True)

        #07 push recent events to gsheet
        min_year = events['year'].max() - WINDOW_YEARS + 1
        recent = events[events['year'] >= min_year].copy()
        db.post_to_gsheet(recent[
            [f for f in EVENT_FIELDS if not f == 'timestamp']], rngcode, 'USER_ENTERED')

        #08 flush csv directory
        gdrive_flush_csv()
        #local_flush_csv()  #deprecated, see note 01


def gdrive_load_csv():  #see note 01
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


def gdrive_flush_csv():  #see note 01
    if len(new_records_asbytes) > 0:
        file_ids = [f['id'] for f in new_records_asbytes]
        db.gdrive.move_files_to_folder(
            file_ids,
            destination_id=GDRIVE_FOLDER_IDS['root'],
            source_id=GDRIVE_FOLDER_IDS['new_records']
        )


def get_tables_from_brecords():  #see note 01
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
            tables['filename'] = df

    return tables


def load_events():
    global events
    events = db.get_table('event')
    has_events = not events is None
    if has_events:
        events.set_index('timestamp', inplace=True)
    return has_events


def events_from_csv(data, filename=''):
    ''' create events from NowThen data table
    '''
    try:
        #01 convert the dates and create activity label
        std = standardForm(data)
        #02 add year, month, week
        events = TimeSeriesTable(std, dtField='timestamp').ts
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
# note 01
#-----------------------------------------------------
# old method used csv files on local pc in combination with Google Backup and Sync (GBS)
# GBS added a security feature to not allow fso scripts to manage file operations
# consequently, the feature was defeated, and replaced by using Google Drive API
# directly to perform file operations in the cloud.

# old methods:

#def local_load_csv():
#    global new_records
#    new_rcd_path = directory_path + DIRECTORY_CONFIG['new_records']
#    new_records = CSVDirectory(new_rcd_path)


#def local_flush_csv():
#    global new_records
#    if new_records.has_files():
#        new_records.flush(directory_path)

#-----------------------------------------------------
# END
#-----------------------------------------------------