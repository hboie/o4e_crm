#!/usr/bin/env python
# coding: utf-8

# # import #
# 

# In[1]:


import json
import re
import sys
from datetime import datetime
from google.cloud import bigquery
from google.oauth2 import service_account
from google.api_core.exceptions import GoogleAPICallError
import schema


# ## read parameter ##

# In[2]:


partner = 'testpartner'
message = 'test log entry'
filename = 'testfile.csv'
date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

if re.match('insert_uploadlog.py', sys.argv[0]):
    if len(sys.argv) >= 1:
        partner = sys.argv[1]
    if len(sys.argv) >= 2:
        message = sys.argv[2]
    if len(sys.argv) >= 3:
        date = sys.argv[3]
    if len(sys.argv) >= 4:
        filename = sys.argv[4]


# ## import configuration ##

# In[3]:


environment = 'prod'
try:
    with open("env") as f:
        environment = f.read()
finally:
    pass

environment


# In[4]:


if environment == 'test':
    config_file = "configuration-test.json"
else:
    config_file = "configuration.json"

with open(config_file) as f:
    config = json.load(f)

config_file


# # connect to database #

# In[5]:


service_account_file = config["google_account_auth"]
credentials = service_account.Credentials.from_service_account_file(
    service_account_file,
    scopes=["https://www.googleapis.com/auth/cloud-platform"]
)

client = bigquery.Client(credentials=credentials, project=credentials.project_id)


# In[6]:


dataset_id = config['dataset_id']
table_id = dataset_id + "." + config['upload_log_table']


# ## upload data ##

# In[7]:


job_config = bigquery.LoadJobConfig(
    schema=schema.schema_uploadlog,
    source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
)

json_data = []

json_entry = {}
json_entry['partner'] = partner
json_entry['date'] = date
json_entry['filename'] = filename
json_entry['message'] = message

json_data.append(json_entry)

try:
    load_job = client.load_table_from_json(
        json_data,
        table_id,
        job_config=job_config,
    )
    result = load_job.result()
    print("upload successfully")
except GoogleAPICallError as e:
    print(f"error: upload failed: {e}")


# In[8]:


client.close()


# In[ ]:




