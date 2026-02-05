#!/usr/bin/env python
# coding: utf-8

# # import #
# 

# In[1]:


import json
import hashlib
import pandas as pd
from pandas.util import hash_pandas_object
import numpy as np
from google.cloud import bigquery
from google.oauth2 import service_account
from google.api_core.exceptions import GoogleAPICallError
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


# # connect to database #

# In[4]:


service_account_file = config["google_account_auth"]
credentials = service_account.Credentials.from_service_account_file(
    service_account_file,
    scopes=["https://www.googleapis.com/auth/cloud-platform"]
)

client = bigquery.Client(credentials=credentials, project=credentials.project_id)


# In[5]:


dataset_id = config['dataset_id']


# ## upload data md_customers ##

# load customers data

# In[6]:


customers_df = pd.read_pickle('./data/customers.pkl').sort_values(by='CUSTOMER_INTERNAL_ID').fillna('')
customers_df.head()


# In[7]:


customers_df['IS_ACTIVE_bool'] = customers_df['IS_ACTIVE'].map(lambda x: True if x.lower() == 'yes' else False)
customers_df['HAS_CONTRACT_bool'] = customers_df['HAS_CONTRACT'].map(lambda x: True if x.lower() == 'yes' else False)
customers_df['HAS_RFQ_bool'] = customers_df['HAS_RFQ'].map(lambda x: True if x.lower() == 'yes' else False)
customers_df = customers_df[['CUSTOMER_INTERNAL_ID', 'CUSTOMER_NAME', 'CUSTOMER_FOLDER_NAME', 'PILOT', 'IS_ACTIVE_bool', 'HAS_CONTRACT_bool', 'HAS_RFQ_bool']]
customers_df['CUSTOMER_NAME'] = customers_df['CUSTOMER_NAME'].str.upper()
customers_df.head()


# In[8]:


cs_customers = hashlib.md5(pd.util.hash_pandas_object(customers_df, index=False).values).hexdigest()
cs_customers


# In[9]:


table_id = dataset_id + "." + config['md_customers_table']


# In[10]:


df = client.list_rows(table_id).to_dataframe().sort_values(by='customer_id').fillna('')
df.head()


# In[11]:


checksum = hashlib.md5(pd.util.hash_pandas_object(df, index=False).values).hexdigest()
checksum


# In[12]:


if cs_customers != checksum:
    # delete all entries
    dml_statement = 'DELETE FROM ' + table_id + ' WHERE TRUE'

    query_job = client.query(dml_statement)
    query_job.result()

    # upload new data
    job_config = bigquery.LoadJobConfig(
        schema=schema.schema_customers,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    )

    json_data = []
    for index, row in customers_df.iterrows():  
        json_entry = {}
        json_entry['customer_id'] = row['CUSTOMER_INTERNAL_ID']
        json_entry['customer_name'] = row['CUSTOMER_NAME']
        json_entry['customer_folder_name'] = row['CUSTOMER_FOLDER_NAME']
        json_entry['pilot'] = row['PILOT']
        json_entry['is_active'] = row['IS_ACTIVE_bool']
        json_entry['has_contract'] = row['HAS_CONTRACT_bool']
        json_entry['has_rfq'] = row['HAS_RFQ_bool']

        json_data.append(json_entry)

    try:
        load_job = client.load_table_from_json(
            json_data,
            table_id,
            job_config=job_config,
        )
        result = load_job.result()
        print(f"{table_id}: upload successfully")
    except GoogleAPICallError as e:
        print(f"{table_id}: error: upload failed: {e}")

else:
    print(f"{table_id}: is up to date")


# ## upload data md_plants ##

# load plants data

# In[13]:


plants_df = pd.read_pickle('./data/plants.pkl').sort_values(by='PLANT_ID').fillna('')
plants_df.head()


# In[14]:


plants_df['PLANT_CLOSED_bool'] = plants_df['PLANT_CLOSED'].map(lambda x: True if x.lower() == 'yes' else False)
plants_df = plants_df[['CUSTOMER_INTERNAL_ID', 'PLANT_ID', 'PLANT_NAME', 'COUNTRY', 'CITY', 'SECTOR', 'PLANT_CLOSED_bool']]
plants_df.head()


# In[15]:


cs_plants = hashlib.md5(pd.util.hash_pandas_object(plants_df, index=False).values).hexdigest()
cs_plants


# upload plants data to database

# In[16]:


table_id = dataset_id + "." + config['md_plants_table']


# In[17]:


df = client.list_rows(table_id).to_dataframe().sort_values(by='plant_id').fillna('')
df.head()


# In[18]:


checksum = hashlib.md5(pd.util.hash_pandas_object(df, index=False).values).hexdigest()
checksum


# In[19]:


if cs_plants != checksum:
    # delete all entries
    dml_statement = 'DELETE FROM ' + table_id + ' WHERE TRUE'

    query_job = client.query(dml_statement)
    query_job.result()

    # upload new data
    job_config = bigquery.LoadJobConfig(
        schema=schema.schema_plants,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    )

    json_data = []
    for index, row in plants_df.iterrows():  
        json_entry = {}
        json_entry['customer_id'] = row['CUSTOMER_INTERNAL_ID']
        json_entry['plant_id'] = row['PLANT_ID']
        json_entry['plant_name'] = row['PLANT_NAME']
        json_entry['country'] = row['COUNTRY']
        json_entry['city'] = row['CITY']
        json_entry['sector'] = row['SECTOR']
        json_entry['plant_closed'] = row['PLANT_CLOSED_bool']        

        json_data.append(json_entry)

    try:
        load_job = client.load_table_from_json(
            json_data,
            table_id,
            job_config=job_config,
        )
        result = load_job.result()
        print(f"{table_id}: upload successfully")
    except GoogleAPICallError as e:
        print(f"{table_id}: error: upload failed: {e}")

else:
    print(f"{table_id}: is up to date")


# ## upload md_productfamilies ##

# load productfamlilies data

# In[20]:


productfamilies_df = pd.read_pickle('./data/productfamilies.pkl').sort_values(by='PRODUCTFAMILY_ID').fillna('')
productfamilies_df.head()


# In[21]:


cs_productfamilies = hashlib.md5(pd.util.hash_pandas_object(productfamilies_df, index=False).values).hexdigest()
cs_productfamilies


# upload productfamilies data to database

# In[22]:


table_id = dataset_id + "." + config['md_productfamilies_table']


# In[23]:


df = client.list_rows(table_id).to_dataframe().sort_values(by='productfamily_id').fillna('')
df.head()


# In[24]:


checksum = hashlib.md5(pd.util.hash_pandas_object(df, index=False).values).hexdigest()
checksum


# In[25]:


if cs_productfamilies != checksum:
    # delete all entries
    dml_statement = 'DELETE FROM ' + table_id + ' WHERE TRUE'

    query_job = client.query(dml_statement)
    query_job.result()

    # upload new data
    job_config = bigquery.LoadJobConfig(
        schema=schema.schema_productfamilies,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    )

    json_data = []
    for index, row in productfamilies_df.iterrows():  
        json_entry = {}
        json_entry['productfamily_id'] = row['PRODUCTFAMILY_ID']
        json_entry['productfamily_code'] = row['PRODUCTFAMILY_CODE']
        json_entry['productfamily_name'] = row['PRODUCTFAMILY_NAME']

        json_data.append(json_entry)

    try:
        load_job = client.load_table_from_json(
            json_data,
            table_id,
            job_config=job_config,
        )
        result = load_job.result()
        print(f"{table_id}: upload successfully")
    except GoogleAPICallError as e:
        print(f"{table_id}: error: upload failed: {e}")

else:
    print(f"{table_id}: is up to date")


# ## upload md_members ##

# load members data

# In[26]:


members_df = pd.read_pickle('./data/members.pkl').sort_values(by='MEMBER_ID').fillna('')
members_df.head()


# In[27]:


cs_members = hashlib.md5(pd.util.hash_pandas_object(members_df, index=False).values).hexdigest()
cs_members


# upload members data to database

# In[28]:


table_id = dataset_id + "." + config['md_members_table']


# In[29]:


df = client.list_rows(table_id).to_dataframe().sort_values(by='member_id').fillna('')
df.head()


# In[30]:


checksum = hashlib.md5(pd.util.hash_pandas_object(df, index=False).values).hexdigest()
checksum


# In[31]:


if cs_members != checksum:
    # delete all entries
    dml_statement = 'DELETE FROM ' + table_id + ' WHERE TRUE'

    query_job = client.query(dml_statement)
    query_job.result()

    # upload new data
    job_config = bigquery.LoadJobConfig(
        schema=schema.schema_members,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    )

    json_data = []
    for index, row in members_df.iterrows():  
        json_entry = {}
        json_entry['member_id'] = row['MEMBER_ID']
        json_entry['member_name'] = row['MEMBER_NAME']
        json_entry['country'] = row['COUNTRY']

        json_data.append(json_entry)

    try:
        load_job = client.load_table_from_json(
            json_data,
            table_id,
            job_config=job_config,
        )
        result = load_job.result()
        print(f"{table_id}: upload successfully")
    except GoogleAPICallError as e:
        print(f"{table_id}: error: upload failed: {e}")

else:
    print(f"{table_id}: is up to date")


# ## upload md_branches ##

# load branches data

# In[32]:


branches_df = pd.read_pickle('./data/branches.pkl').sort_values(by='BRANCH_ID').fillna('')
branches_df.head()


# In[33]:


branches_df['BRANCH_CLOSED_bool'] = branches_df['BRANCH_CLOSED'].map(lambda x: True if x.lower() == 'yes' else False)
branches_df = branches_df[['MEMBER_ID', 'BRANCH_ID', 'BRANCH_NAME', 'MEMBER_NAME', 'BRANCH_CLOSED_bool']]
branches_df.head()


# In[34]:


cs_branches = hashlib.md5(pd.util.hash_pandas_object(branches_df, index=False).values).hexdigest()
cs_branches


# upload branches to database

# In[35]:


table_id = dataset_id + "." + config['md_branches_table']


# In[36]:


df = client.list_rows(table_id).to_dataframe().sort_values(by='branch_id').fillna('')
df.head()


# In[37]:


checksum = hashlib.md5(pd.util.hash_pandas_object(df, index=False).values).hexdigest()
checksum


# In[38]:


if cs_branches != checksum:
    # delete all entries
    dml_statement = 'DELETE FROM ' + table_id + ' WHERE TRUE'

    query_job = client.query(dml_statement)
    query_job.result()

    # upload new data
    job_config = bigquery.LoadJobConfig(
        schema=schema.schema_branches,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    )

    json_data = []
    for index, row in branches_df.iterrows():  
        json_entry = {}
        json_entry['branch_id'] = row['BRANCH_ID']
        json_entry['branch_name'] = row['BRANCH_NAME']
        json_entry['member_id'] = row['MEMBER_ID']
        json_entry['member_name'] = row['MEMBER_NAME']
        json_entry['branch_closed'] = row['BRANCH_CLOSED_bool']

        json_data.append(json_entry)

    try:
        load_job = client.load_table_from_json(
            json_data,
            table_id,
            job_config=job_config,
        )
        result = load_job.result()
        print(f"{table_id}: upload successfully")
    except GoogleAPICallError as e:
        print(f"{table_id}: error: upload failed: {e}")

else:
    print(f"{table_id}: is up to date")


# ## upload md_suppliers ##

# load suppliers data

# In[39]:


suppliers_df = pd.read_pickle('./data/suppliers.pkl').sort_values(by='SUPPLIER_ID').fillna('')
suppliers_df.head()


# In[40]:


cs_suppliers = hashlib.md5(pd.util.hash_pandas_object(suppliers_df, index=False).values).hexdigest()
cs_suppliers


# upload suppliers data to database

# In[41]:


table_id = dataset_id + "." + config['md_suppliers_table']


# In[42]:


df = client.list_rows(table_id).to_dataframe().sort_values(by='supplier_id').fillna('')
df.head()


# In[43]:


checksum = hashlib.md5(pd.util.hash_pandas_object(df, index=False).values).hexdigest()
checksum


# In[44]:


if cs_suppliers != checksum:
    # delete all entries
    dml_statement = 'DELETE FROM ' + table_id + ' WHERE TRUE'

    query_job = client.query(dml_statement)
    query_job.result()

    # upload new data
    job_config = bigquery.LoadJobConfig(
        schema=schema.schema_suppliers,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    )

    json_data = []
    for index, row in suppliers_df.iterrows():  
        json_entry = {}
        json_entry['supplier_id'] = row['SUPPLIER_ID']
        json_entry['supplier_name'] = row['SUPPLIER_NAME']

        json_data.append(json_entry)

    try:
        load_job = client.load_table_from_json(
            json_data,
            table_id,
            job_config=job_config,
        )
        result = load_job.result()
        print(f"{table_id}: upload successfully")
    except GoogleAPICallError as e:
        print(f"{table_id}: error: upload failed: {e}")

else:
    print(f"{table_id}: is up to date")


# In[45]:


client.close()


# In[ ]:




