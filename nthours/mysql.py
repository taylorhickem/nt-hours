""" this script tests connection to the remote mysql server
"""
# -----------------------------------------------------
# Import
# -----------------------------------------------------
import json
import pymysql
from sqlalchemy import create_engine

##-----------------------------------------------------
# Module variables
##-----------------------------------------------------

MYSQL_CONFIG = {}
MYSQL_DB_NAME = 'hours'
MYSQL_CREDENTIALS = {}
engine = None
con = None

# -----------------------------------------------------
# Setup
# -----------------------------------------------------
def load():
    load_config()
    load_sql()


def load_config():
    global MYSQL_CONFIG, MYSQL_CREDENTIALS
    MYSQL_CREDENTIALS = json.load(open('mysql_credentials.json'))


def load_sql():
    global engine, con
    if engine is None:
        engine = create_engine(
            MYSQL_CREDENTIALS['login'].format(
                user=MYSQL_CREDENTIALS['username'],
                pw=MYSQL_CREDENTIALS['password'],
                db=MYSQL_CREDENTIALS['database']))
        con = engine.connect()