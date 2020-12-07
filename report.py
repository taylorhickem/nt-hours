'''time tracking reports from AngryAztec NowThen app user data
'''
# -----------------------------------------------------
# Import
# -----------------------------------------------------
import sys

import nowthen as nt
# -----------------------------------------------------
# Module variables
# -----------------------------------------------------
# -----------------------------------------------------
# Setup
# -----------------------------------------------------

# -----------------------------------------------------
# Reports
# -----------------------------------------------------


def update_events():
    '''Update events database sqlite and gsheet with new NowThen records
    '''
    nt.load()
    nt.update_events()

def update_activity_report():
    pass


# -----------------------------------------------------
# Command line interface
# -----------------------------------------------------
def autorun():
    if len(sys.argv)>1:
        process_name = sys.argv[1]
        if process_name == 'update_events':
            update_events()
        elif process_name == 'update_activity_report':
            update_activity_report()
    else:
        print('no report specified')

if __name__ == "__main__":
    autorun()
# -----------------------------------------------------
# ***
# -----------------------------------------------------