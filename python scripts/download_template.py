#!/usr/bin/env python
# coding: utf-8

# ## Import ##

# In[1]:


import json
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.oauth2 import service_account
import io


# ## load configuration ##

# In[2]:


environment = 'prod'
try:
    with open("env") as f:
        environment = f.read()
finally:
    pass

environment


# In[3]:


if environment == 'test':
    config_file = "configuration-test.json"
else:
    config_file = "configuration.json"

with open(config_file) as f:
    config = json.load(f)

config_file


# ## Connect to google drive ##

# In[4]:


service_account_file = config["google_account_auth"]
credentials = service_account.Credentials.from_service_account_file(
    service_account_file,
    scopes=['https://www.googleapis.com/auth/spreadsheets.readonly']
)

drive_service = build('drive', 'v3', credentials=credentials)


# ## download template ##

# In[5]:


spreadsheet_id = config['template_spreadsheet_id']

request = drive_service.files().export_media(
    fileId=spreadsheet_id,
    mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
)

fh = io.FileIO('output.xlsx', 'wb')
downloader = MediaIoBaseDownload(fh, request)

done = False
while not done:
    status, done = downloader.next_chunk()

print(f"Download finished.")

