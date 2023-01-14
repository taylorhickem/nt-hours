"""converts delimter file type and extension
>venv\Scripts\python -m nthours.delimiter_convert " # " , csv
"""
import os
import sys
import pandas as pd
import datetime as dt


FROM_DELIM = ' # '

HEADER = 'End time,Start time,Client,Project,Tags,Description,Start date\n'
CSV_EXT = 'csv'
CSV_DELIM = ','
REM_DELIM = '@'

CLIENT_DEFAULT = '03 DBS Cloud Developer'
USER = 'Taylor Hickem'
TASK = None
BILLABLE = 'No'
EMAIL = 'taylor.hickem@gmail.com'
FIELDS = [
    'User',
    'Email',
    'Client',
    'Project',
    'Task',
    'Description',
    'Billable',
    'Start date',
    'Start time',
    'Duration',
    'Tags'
]


def convert(file_path):
    with open(file_path, 'r') as f:
        source = f.readlines()
    f.close()

    date_seq = file_path[-12:-4]
    file_date = '-'.join([date_seq[:4], date_seq[4:6], date_seq[6:]])

    converted = HEADER
    for i in range(len(source)):
        l = source[i]
        ln = None
        if i < (len(source)-1):
            ln = source[i+1]
        l = l[:5] + ':00' + l[5:]   # pad the timestamp with trailing :00 seconds
        if ln:
            l = ln[:5] + ':00 # ' + l # add end time to the beginning of the line
        else:
            l = l[:5] + ':00 # ' + l
        l = l[:-1] + ' # ' + file_date + '\n'  # add the date to the end of the line
        converted = converted + l.replace(FROM_DELIM, CSV_DELIM)

    with open(file_path, 'w') as f:
        f.write(converted)
    f.close()


def change_file_extension(file_path, file_extension):
    new_file = file_path[:file_path.find('.')+1] + file_extension
    os.rename(file_path, new_file)


def csv_format(file_path):
    events = pd.read_csv(file_path)
    events = events[~events['Client'].str.contains(REM_DELIM)].copy()
    events['Client'] = events['Client'].apply(lambda x: CLIENT_DEFAULT if x == '.' else x)
    events['Duration'] = events.apply(lambda x: duration_str(x['Start time'], x['End time']), axis=1)
    events['Email'] = EMAIL
    events['User'] = USER
    events['Billable'] = BILLABLE
    events['Task'] = TASK
    events = events[FIELDS]
    events.to_csv(file_path, index=False)


def duration_str(start_time, end_time):
    TIME_FORMAT = '%H:%M:%S'
    start_dt = dt.datetime.strptime(start_time, TIME_FORMAT)
    end_dt = dt.datetime.strptime(end_time, TIME_FORMAT)
    del_dt = end_dt - start_dt
    t = dt.date.today()
    td = dt.datetime(t.year, t.month, t.day)
    dur_dt = td + del_dt
    return dur_dt.strftime(TIME_FORMAT)


if __name__ == '__main__':
    if len(sys.argv) > 1:
        file_path = [f for f in os.listdir() if f.endswith('.hsv')][0]
        FROM_DELIM = sys.argv[1]
        convert(file_path)
        change_file_extension(file_path, CSV_EXT)
        csv_path = file_path[:-3] + CSV_EXT
        csv_format(csv_path)
