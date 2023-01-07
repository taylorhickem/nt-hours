# nt-hours
time tracking report using [Toggl Track](https://track.toggl.com/timer) time tracker app manual upload to Google Drive, Google Sheets for reporting and MySQL Server to store the data

For details, see the app documentation in Notion: [hoursApp](https://rightful-sweater-058.notion.site/hoursApp-1bb08e7c9e8d45b38fb832a3b3422771)

## setup

1. install the module into your python project from the github repository url using pip

     `pip install git+https://github.com/taylorhickem/nt-hours.git`

2. configure your client specific information files 

     /root directory
     
        /gsheet
            client_secret.json
        config.json
        gdrive_config.json 
        gsheet_config.json
        mysql_credentials.json 
 
                 
3. create your google spreadsheet

    take note of 
    * the _workbookId_, read from the url ../spreadsheets/d/_workbookId_
    * the ranges of interest and give them names

4. grant write access to your service account

   select _share_ and add your _client_email_ with write priviledges for your sheet and your google drive folders

6. configure your _gsheet_config.json_ file to match your spreadsheet

## sample code

update from terminal

`\myapp>python report.py update_events`

run from python interpreter

```
import report
report.update_events()
```

load and get _events_ as a pandas _DataFrame_

```
import pandas as pd
from nthours import nowthen as nt

nt.load()
events = nt.db.get_table('event')
```

## sample config files

 ... to be updated ...

  _client_secret.json_

  _config.json_

  _gdrive_config.json_

  _gsheet_config.json_

  _mysql_credentials.json_ 
    
```
    {
... 
}
```

