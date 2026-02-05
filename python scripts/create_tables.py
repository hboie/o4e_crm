#!/usr/bin/env python
# coding: utf-8

# # imports

# In[1]:


import json
from google.cloud import bigquery
from google.oauth2 import service_account
import schema


# ## import configuration ##

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


# ## get client ##

# In[4]:


service_account_file = config["google_account_auth"]
credentials = service_account.Credentials.from_service_account_file(
    service_account_file,
    scopes=["https://www.googleapis.com/auth/cloud-platform"]
)

client = bigquery.Client(credentials=credentials, project=credentials.project_id)


# ## define DataSet-ID ##

# In[5]:


dataset_id = config['dataset_id']


# ## create DataSet ##

# In[18]:


dataset = bigquery.Dataset(dataset_id)
dataset.location = "EU"  # oder z.B. "US"

dataset = client.create_dataset(dataset, exists_ok=True)

print(f"Created dataset {dataset.dataset_id}")


# ## create table turnover ##

# In[19]:


table_id = dataset_id + "." + config['turnover_table']

table = bigquery.Table(table_id, schema=schema.schema_turnover)
table = client.create_table(table)
print(f"Created table {table.project}.{table.dataset_id}.{table.table_id}")


# ## create table uploadlog ##

# In[20]:


table_id = dataset_id + "." + config['upload_log_table']

table = bigquery.Table(table_id, schema=schema.schema_uploadlog)
table = client.create_table(table)
print(f"Created table {table.project}.{table.dataset_id}.{table.table_id}")


# ## create table md_customers ##

# In[21]:


table_id = dataset_id + "." + config['md_customers_table']

table = bigquery.Table(table_id, schema=schema.schema_customers)
table = client.create_table(table)
print(f"Created table {table.project}.{table.dataset_id}.{table.table_id}")


# ## create table md_plants ##

# In[6]:


table_id = dataset_id + "." + config['md_plants_table']

table = bigquery.Table(table_id, schema=schema.schema_plants)
table = client.create_table(table)
print(f"Created table {table.project}.{table.dataset_id}.{table.table_id}")


# ## create table md_productfamilies ##

# In[23]:


table_id = dataset_id + "." + config['md_productfamilies_table']

table = bigquery.Table(table_id, schema=schema.schema_productfamilies)
table = client.create_table(table)
print(f"Created table {table.project}.{table.dataset_id}.{table.table_id}")


# ## create table md_members ##

# In[24]:


table_id = dataset_id + "." + config['md_members_table']

table = bigquery.Table(table_id, schema=schema.schema_members)
table = client.create_table(table)
print(f"Created table {table.project}.{table.dataset_id}.{table.table_id}")


# ## create table md_branches ##

# In[7]:


table_id = dataset_id + "." + config['md_branches_table']

table = bigquery.Table(table_id, schema=schema.schema_branches)
table = client.create_table(table)
print(f"Created table {table.project}.{table.dataset_id}.{table.table_id}")


# ## create table md_suppliers ##

# In[28]:


table_id = dataset_id + "." + config['md_suppliers_table']

table = bigquery.Table(table_id, schema=schema.schema_suppliers)
table = client.create_table(table)
print(f"Created table {table.project}.{table.dataset_id}.{table.table_id}")


# In[29]:


client.close()


# In[ ]:




