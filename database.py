''' interface with data sources
'''
# -----------------------------------------------------
# Import
# -----------------------------------------------------
import os
import shutil
import utilities.fso as fso
import json
import pandas as pd
import datetime as dt
from sqlalchemy import create_engine
from sqlalchemy.engine.reflection import Inspector
import gsheet.api as gs

##-----------------------------------------------------
# Module variables
##-----------------------------------------------------
# constants
NUMERIC_TYPES = ['int', 'float']
SQL_DB_NAME = 'sqlite:///hours.db'


# dynamic : config
CONFIG = {}
GSHEET_CONFIG = {}


# custom class objects from other modules
engine = None
gs_engine = None


# -----------------------------------------------------
# Setup
# -----------------------------------------------------
def load():
    load_config()
    load_sql()
    load_gsheet()


def load_config():
    global CONFIG, GSHEET_CONFIG
    CONFIG = json.load(open('config.json'))
    GSHEET_CONFIG = json.load(open(CONFIG['gsheet_config_file']))


def load_gsheet():
    global gs_engine
    if gs_engine is None:
        gs_engine = gs.SheetsEngine()


# -----------------------------------------------------
# Sqlite
# -----------------------------------------------------

def load_sql():
    global engine, inspector, table_names
    if engine is None:
        engine = create_engine(SQL_DB_NAME, echo=False)
        inspector = Inspector.from_engine(engine)
        table_names = inspector.get_table_names()


def table_exists(tableName):
    return tableName in table_names


def get_table(tableName):
    if table_exists(tableName):
        tbl = pd.read_sql_table(tableName,con=engine)
    else:
        tbl = None
    return tbl


def update_table(tbl,tblname,append=True):
    global engine
    if append:
        ifex = 'append'
    else:
        ifex = 'replace'
    tbl.to_sql(tblname, con=engine, if_exists=ifex, index=False)


# -----------------------------------------------------
# Google spreadsheet
# -----------------------------------------------------

def get_sheet(rng_code):
    wkbid = GSHEET_CONFIG['wkbid']
    rng_config = GSHEET_CONFIG[rng_code]
    rngid = rng_config['data'];
    hdrid = rng_config['header']
    valueList = gs_engine.get_rangevalues(wkbid, rngid)
    header = gs_engine.get_rangevalues(wkbid, hdrid)[0]
    rng = pd.DataFrame(valueList, columns=header)
    if 'data_types' in rng_config:
        data_types = rng_config['data_types']
        for field in data_types:
            typeId = data_types[field]
            if not typeId in ['str', 'date']:
                if typeId in NUMERIC_TYPES:
                    # to deal with conversion from '' to nan
                    if typeId in ['float']:  # nan compatible
                        rng[field] = pd.to_numeric(rng[field]).astype(typeId)
                    else:  # nan incompatible types
                        rng[field] = pd.to_numeric(rng[field]).fillna(0).astype(typeId)
                else:
                    rng[field] = rng[field].astype(typeId)
            if typeId == 'date':
                if 'date_format' in rng_config:
                    rng[field] = rng[field].apply(
                        lambda x: dt.datetime.strptime(x, rng_config['date_format']))
    return rng


def post_to_gsheet(df,rng_code,input_option='RAW'):
    #values is a 2D list [[]]
    wkbid = GSHEET_CONFIG['wkbid']
    rngid = GSHEET_CONFIG[rng_code]['data']
    values = df.values.astype('str').tolist()
    gs_engine.clear_rangevalues(wkbid,rngid)
    #write values - this method writes everything as a string
    gs_engine.set_rangevalues(wkbid,rngid,values,input_option)


# -----------------------------------------------------
# CSV file directory
# -----------------------------------------------------


class CSVDirectory(object):
    def __init__(self,directory_path):
        self.path = directory_path
        self.load_files()
    def load_files(self):
        self.files = fso.getFilesInFolder(self.path)
        if len(self.files)>0:
            self.csv_files = [f for f in self.files if '.csv' in f]
    def get_tables(self):
        tbls = {}
        if len(self.csv_files)>0:
            for f in self.csv_files:
                try:
                    df = pd.read_csv(self.path + '\\' + f)
                    tbls[f] = df
                except:
                    pass
        return tbls
    def has_files(self):
        return len(self.files)>0
    def has_csv(self):
        csv_check = False
        if self.has_files():
            csv_check = len(self.csv_files)>0
        return csv_check
    def flush(self,new_directory=None):
        if self.has_files():
            for f in self.files:
                src = self.path + '\\' + f
                if new_directory is None:
                    os.remove(src)
                else:
                    if new_directory == '':
                        dest = f
                    else:
                        dest = new_directory + '\\' + f
                    shutil.move(src,dest)
            self.__init__(self.path)

# -----------------------------------------------------
# ***
# -----------------------------------------------------
