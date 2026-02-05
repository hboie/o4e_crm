#!/usr/bin/env python
# coding: utf-8

# ## Import ##

# In[7]:


import json
import pandas as pd
import numpy as np
from googleapiclient.discovery import build
from google.oauth2 import service_account


# ## load configuration ##

# In[8]:


environment = 'prod'
try:
    with open("env") as f:
        environment = f.read()
finally:
    pass

environment


# In[9]:


if environment == 'test':
    config_file = "configuration-test.json"
else:
    config_file = "configuration.json"

with open(config_file) as f:
    config = json.load(f)

config_file


# ## Connect to google drive ##

# In[10]:


service_account_file = config["google_account_auth"]
credentials = service_account.Credentials.from_service_account_file(
    service_account_file,
    scopes=['https://www.googleapis.com/auth/spreadsheets.readonly']
)

sheets_service = build('sheets', 'v4', credentials=credentials)


# ## access master data file ##

# In[11]:


spreadsheet_id = config['md_spreadsheet_id']
targets =[
    { 'sheet_name': 'MD_CUSTOMERS', 'columns': 8, 'filename': './data/customers.pkl' },
    { 'sheet_name': 'MD_PLANTS', 'columns': 8, 'filename': './data/plants.pkl' },
    { 'sheet_name': 'MD_MEMBERS', 'columns': 3, 'filename': './data/members.pkl' },
    { 'sheet_name': 'MD_BRANCHES', 'columns': 5, 'filename': './data/branches.pkl' },
    { 'sheet_name': 'MD_SUPPLIERS', 'columns': 2, 'filename': './data/suppliers.pkl' },
    { 'sheet_name': 'MD_PRODUCTFAMILIES', 'columns': 3, 'filename': './data/productfamilies.pkl' },
    { 'sheet_name': 'MD_MONTHS', 'columns': 3, 'filename': './data/months.pkl' },
]


# In[13]:


for target in targets:
    result = sheets_service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id, range=target['sheet_name']).execute()
    values = result.get('values', [])

    if not values:
        print(target['sheet_name'] + ': No data found.')
    else:
        columns = values[0][:target['columns']]
        data = [
            row + [None] * (target['columns'] - len(row))
            for row in values[1:]
        ]

        data_df = pd.DataFrame(data, columns=columns)

        data_df.head()
        data_df.info()

        data_df.to_pickle(target['filename'])

        print(target['sheet_name'] + ': saved data to file ' + target['filename'])


# In[ ]:





# In[ ]:




