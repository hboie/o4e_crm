#!/usr/bin/env python
# coding: utf-8

# # import #

# In[89]:


import re
import sys
from pathlib import Path
import pandas as pd
from openpyxl import load_workbook


# # shell call #

# In[90]:


partner = 'efrapo'
base_dir = 'data'
data_dir = 'template'

if re.match('create_project_partner_crm_db.py', sys.argv[0]):
    if len(sys.argv) >= 1:
        partner = sys.argv[1]
    if len(sys.argv) >= 2:
        base_dir = sys.argv[2]
    if len(sys.argv) >= 3:
        data_dir = sys.argv[3]

folder = Path(base_dir) / data_dir
folder.mkdir(exist_ok=True)

filename = f"{folder}/{partner}_crm_db.xlsx" 


# # load data #

# ## load project partner data ##

# In[91]:


ppartner_df = pd.read_pickle('./data/projectpartners.pkl')
ppartner_df.head()


# In[92]:


customer_list = ppartner_df.loc[ppartner_df["PROJECT_PARTNER_NAME"] == partner, "CUSTOMER_INTERNAL_ID"].tolist()

print(customer_list)

if len(customer_list) == 0:
    sys.exit(0)


# In[93]:


member_ids = ppartner_df.loc[ppartner_df["PROJECT_PARTNER_NAME"] == partner, "MEMBER_ID"]

member_id = member_ids.iloc[0] if not member_ids.empty else None

print(member_id)

if member_id == None:
    sys.exit(0)


# ## load master data ##

# In[94]:


customers_df = pd.read_pickle('./data/customers.pkl')
customers_df.head()


# In[95]:


plants_df = pd.read_pickle('./data/plants.pkl')
plants_df.head()


# In[96]:


productfamilies_df = pd.read_pickle('./data/productfamilies.pkl')
productfamilies_df.head()


# In[97]:


branches_df = pd.read_pickle('./data/branches.pkl')
branches_df.head()


# In[98]:


suppliers_df = pd.read_pickle('./data/suppliers.pkl')
suppliers_df.head()


# # get reduced master data #

# In[99]:


customers_red_df = customers_df[customers_df['CUSTOMER_INTERNAL_ID'].isin(customer_list)]
customers_red_df.head()


# In[100]:


plants_red_df = plants_df[plants_df['CUSTOMER_INTERNAL_ID'].isin(customer_list)]
plants_red_df.head()


# In[101]:


branches_red_df = branches_df[branches_df['MEMBER_ID'] == member_id]
branches_red_df.head()


# # create project partner crm_db #

# In[102]:


with pd.ExcelWriter(filename, engine="openpyxl") as writer:
    customers_red_df.to_excel(writer, sheet_name="MD_CUSTOMER", index=False)
    plants_red_df.to_excel(writer, sheet_name="MD_PLANTS", index=False)
    branches_red_df.to_excel(writer, sheet_name="MD_BRANCHES", index=False)
    suppliers_df.to_excel(writer, sheet_name="MD_SUPPLIERS", index=False)    
    productfamilies_df.to_excel(writer, sheet_name="MD_PRODUCTFAMILIES", index=False)


# In[103]:


wb = load_workbook(filename)

for ws in wb.worksheets: 
    for col in ws.columns:
        max_length = 0
        col_letter = col[0].column_letter

        for cell in col:
            if cell.value:
                max_length = max(max_length, len(str(cell.value)))

        ws.column_dimensions[col_letter].width = max_length + 4

wb.save(filename)

print(f"created file {filename}")


# In[ ]:




