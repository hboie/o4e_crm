#!/usr/bin/env python
# coding: utf-8

# # import #
# 

# In[340]:


import json
import re
import sys
import chardet
from datetime import datetime
import pandas as pd
import numpy as np
from google.cloud import bigquery
from google.oauth2 import service_account
from google.api_core.exceptions import GoogleAPICallError
import schema


# ## import configuration ##

# In[341]:


environment = 'prod'
try:
    with open("env") as f:
        environment = f.read()
finally:
    pass

environment


# In[342]:


if environment == 'test':
    config_file = "configuration-test.json"
else:
    config_file = "configuration.json"

with open(config_file) as f:
    config = json.load(f)

config_file


# # import data file #
# 
# try to load data from template without headers

# In[343]:


import_file = 'CA_VERZOLLA_CustomerTurnover_20260205.csv'
log_file = 'import.log'
partner = 'testpartner'
date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
partner_config_file = 'partner_config.json'

if re.match('import_turnover.py', sys.argv[0]):
    if len(sys.argv) >= 1:
        import_file = sys.argv[1]
    if len(sys.argv) >= 2:
        log_file = sys.argv[2]
    if len(sys.argv) >= 3:
        partner = sys.argv[3]
    if len(sys.argv) >= 4:
        date = sys.argv[4]
    if len(sys.argv) >= 5:
        partner_config_file = sys.argv[5]


# import data from file

# In[344]:


with open(log_file, "w") as logfile:
    logfile.write(f"opening file {import_file} for reading\r\n")


# detect encoding

# In[345]:


sample_size=100000
with open(import_file, "rb") as f:
    rawdata = f.read(sample_size)
encoding = chardet.detect(rawdata)['encoding']

if encoding == 'ascii':
    encoding = 'latin-1'

with open(log_file, "a") as logfile:
    logfile.write(f"using encoding {encoding}\r\n")


# define import setting

# In[346]:


column_names = ['code_of_integration', 'member_branch_id', 'plant_id', 'product_family_id', 'supplier_id', 'product_id', 
                'supplier_product_reference', 'product_description', 'unit_net_price', 'quantity', 'turnover', 'price_per', 'sales_unit',
                'delivery_date', 'invoice_date', 'year', 'month', 'quarter', 'semester', 'member_order_nb', 'customer_order_nb', 'deliver_note',
                'invoice_nb', 'customer_reference', 'member_internal_reference', 'buying_member_id', 'member_id']
kwargs = {}
kwargs['filepath_or_buffer'] = import_file
kwargs['header'] = None
kwargs['index_col'] = False
kwargs['encoding'] = encoding
kwargs['names'] = column_names
kwargs['delimiter'] = ';'
kwargs['skiprows'] = 0


# In[347]:


if partner_config_file != '':
    with open(partner_config_file) as f:
        partner_config = json.load(f)

        if 'delimiter' in partner_config:
            kwargs['delimiter'] = partner_config['delimiter']
            print(f"using delimiter '{kwargs['delimiter']}'")

        if 'column_names' in partner_config:
            kwargs['names'] = partner_config['column_names']
            print(f"using columns '{kwargs['names']}'")

        if 'skiprows' in partner_config:
            kwargs['skiprows'] = partner_config['skiprows']
            print(f"skip '{kwargs['skiprows']}' leading rows")

        if 'quotechar' in partner_config:
            kwargs['quotechar'] = partner_config['quotechar']
            print(f"using qoutechar '{kwargs['quotechar']}'")


# In[348]:


inp_df = pd.read_csv(**kwargs)


# In[349]:


inp_df.head(10)


# In[350]:


inp_df.info()


# In[351]:


with open(log_file, "a") as logfile:
    logfile.write(f"read {len(inp_df)} lines\r\n")


# ## clean data ##

# In[352]:


inp_df = inp_df.fillna('')
inp_df['code_of_integration']=inp_df['code_of_integration'].astype("string")
inp_df['member_branch_id']=inp_df['member_branch_id'].astype("string")
inp_df['plant_id']=inp_df['plant_id'].astype("string")
inp_df['product_family_id']=inp_df['product_family_id'].astype("string")
inp_df['supplier_product_reference']=inp_df['supplier_product_reference'].astype("string")
inp_df['product_description']=inp_df['product_description'].astype("string")
inp_df['supplier_product_reference']=inp_df['supplier_product_reference'].astype("string")
inp_df['member_order_nb']=inp_df['member_order_nb'].astype("string")
inp_df['customer_order_nb']=inp_df['customer_order_nb'].astype("string")
inp_df['deliver_note']=inp_df['deliver_note'].astype("string")
inp_df['invoice_nb']=inp_df['invoice_nb'].astype("string")
inp_df['customer_reference']=inp_df['customer_reference'].astype("string")
inp_df['member_internal_reference']=inp_df['member_internal_reference'].astype("string")
inp_df['member_id']=inp_df['member_id'].astype("string")

missing_columns = [c for c in column_names if c not in set(kwargs['names'])]
missing_mandatory = []
missing_empty = []
for col in missing_columns:
    if col in ['product_id', 'sales_unit', 'delivery_date', 'quarter', 'semester', 'member_order_nb', 'customer_order_nb', 'deliver_note',
                'invoice_nb', 'customer_reference', 'member_internal_reference', 'buying_member_id']:
        inp_df[col] = ''
        missing_empty.append("'" + col + "'")
    elif col in ['unit_net_price', 'price_per']:
        inp_df[col] = 0.0
        missing_empty.append("'" + col + "'")
    else:
        missing_mandatory.append("'" + col + "'")

message = ''
if len(missing_empty) > 0:
    missing_empty_str = ", ".join(missing_empty)
    message += f"added empty fields {missing_empty_str}\r\n"
if len(missing_mandatory) > 0 :
    missing_mandatory_str = ", ".join(missing_mandatory)
    message += f"mandatory fields {missing_mandatory_str} missing\r\n"

if message:
    with open(log_file, "a") as logfile:
        logfile.write(message)


# In[353]:


inp_df.head(10)


# ## verify data ##

# load master data

# In[354]:


plants_df = pd.read_pickle('./data/plants.pkl')
plants_df.head()


# In[355]:


productfamilies_df = pd.read_pickle('./data/productfamilies.pkl')
productfamilies_df.head()


# In[356]:


members_df = pd.read_pickle('./data/members.pkl')
members_df.head()


# In[357]:


branches_df = pd.read_pickle('./data/branches.pkl')
branches_df.head()


# In[358]:


suppliers_df = pd.read_pickle('./data/suppliers.pkl')
suppliers_df.head()


# In[359]:


months_df = pd.read_pickle('./data/months.pkl')
months_df.head()


# create new dataframe

# In[360]:


df = pd.DataFrame({
    "code_of_integration": pd.Series(dtype="str"),
    "customer_id": pd.Series(dtype="str"),
    "plant_id": pd.Series(dtype="str"),
    "member_id": pd.Series(dtype="str"),
    "member_branch_id": pd.Series(dtype="str"),
    "product_family_id": pd.Series(dtype="str"),
    "quantity": pd.Series(dtype="float"),
    "turnover": pd.Series(dtype="float"),
    "supplier_product_reference": pd.Series(dtype="str"),
    "product_description": pd.Series(dtype="str"),
    "supplier_id": pd.Series(dtype="str"),
    "invoice_date": pd.Series(dtype="str"),
    "year": pd.Series(dtype="int"),
    "month": pd.Series(dtype="int"),
    "customer_order_nb": pd.Series(dtype="str"),
    "member_order_nb": pd.Series(dtype="str")
})


# loop through dataframe and verify data

# In[361]:


count_import = 0
count_reject = 0
count_member_to_member = 0
count_purchase = 0

reject_msg = []

for index, row in inp_df.iterrows():
    import_row = True
    reject_reason = []

    buying_member_id = row['buying_member_id'].strip()
    member_id = row['member_id'].strip()

    # drop member-to-member turnover
    if buying_member_id != "" and member_id != "":
        count_member_to_member += 1
        continue

    # drop supplier turnover
    plant_id = row['plant_id'].strip()
    product_family_code = row['product_family_id'].strip()
    branch_id = row['member_branch_id'].strip()

    if plant_id == "" and product_family_code == "" and branch_id == "":
        count_purchase += 1
        continue

    # check invoice date
    invoice_date = row['invoice_date'].strip()

    if invoice_date == "":
        import_row = False
        reject_reason.append('inovice date must not be empty')

    elif not re.fullmatch(r"\d{4}-\d{2}-\d{2}", invoice_date):
        import_row = False
        reject_reason.append('inovice date ' + invoice_date + ' is not valid')

    # check plant id
    customer_id = ""

    if plant_id == "":
        import_row = False
        reject_reason.append('plant id must not be empty')
    else:
        plant_entries = plants_df[plants_df['PLANT_ID'] == plant_id]
        if len(plant_entries) == 0:
            import_row = False
            reject_reason.append('plant id ' + row['plant_id'] + ' is not valid')
        else:
            customer_id = plant_entries.iloc[0]['CUSTOMER_INTERNAL_ID']

    # check product family id
    product_family_code = row['product_family_id'].strip()

    if product_family_code == "":
        import_row = False
        reject_reason.append('product family id must not be empty')
    else:
        family_entries = productfamilies_df[productfamilies_df['PRODUCTFAMILY_ID'] == product_family_code]
        if len(family_entries) > 0:
            product_family_code = family_entries.iloc[0]['PRODUCTFAMILY_CODE']
        else:
            if len(product_family_code) < 2:
                product_family_code = product_family_code.zfill(2)

            if not (productfamilies_df['PRODUCTFAMILY_CODE'] == product_family_code).any():
                import_row = False
                reject_reason.append('product family ' + row['product_family_id'] + ' is not valid')

    # check member id
    if member_id == "":
        import_row = False
        reject_reason.append('member id must not be empty')
    else:
        if not (members_df['MEMBER_ID'] == member_id).any():
            import_row = False
            reject_reason.append('member ' + member_id + ' is not valid')

    # check branch id
    branch_id = row['member_branch_id'].strip()

    if branch_id == "":
        import_row = False
        reject_reason.append('branch id must not be empty')
    elif not (branches_df['BRANCH_ID'] == branch_id).any():
        import_row = False
        reject_reason.append('member branch ' + row['member_branch_id'] + ' is not valid')

    # check supplier id
    supplier_id = row['supplier_id'].strip()

    if supplier_id == "":
        import_row = False
        reject_reason.append('supplier id must not be empty')
    elif not (suppliers_df['SUPPLIER_ID'] == supplier_id).any():
        import_row = False
        reject_reason.append('supplier ' + row['supplier_id'] + ' is not valid')

    # check year
    year = row['year']

    if year == "":
        import_row = False
        reject_reason.append('year must not be empty')
    elif isinstance(year, (int, float)):
        year = int(year)
    else:
        try:
            year = int(year)
        except:
            import_row = False
            reject_reason.append('year ' + row['year'] + ' is not valid')

    # check month
    month = row['month']

    if month == "":
        import_row = False
        reject_reason.append('month must not be empty')
    elif isinstance(month, (int, float)):
        month = int(row['month'])
        if month < 1 or month > 12:
            import_row = False
            reject_reason.append('month ' + row['month'] + ' is not valid')
    else:
        month_entries = months_df[months_df['MONTH_ID'] == month]
        if len(month_entries) > 0:
            month = int(month_entries.iloc[0]['MONTH_CODE'])
        else:
            import_row = False
            reject_reason.append('month ' + row['month'] + ' is not valid')

    # check quantity
    quantity = row['quantity']
    if isinstance(quantity, (int, float)):
        quantity = round(quantity,2)
    else:
        try:
            quantity = round(float(quantity),2)
        except:
            import_row = False
            reject_reason.append('quantity ' + row['quantity'] + ' is not valid')

    # check turnover
    turnover = row['turnover']
    if isinstance(turnover, (int, float)):
        turnover = round(turnover,2)
    else:
        try:
            turnover = round(float(turnover),2)
        except:
            import_row = False
            reject_reason.append('turnover ' + row['turnover'] + ' is not valid')

    if import_row:
        new_row = {
            "code_of_integration" : row['code_of_integration'],
            "customer_id": customer_id,
            "plant_id": plant_id,
            "member_id": member_id,
            "member_branch_id": branch_id,
            "product_family_id": product_family_code,
            "quantity": quantity,
            "turnover": turnover,
            "supplier_product_reference": row['supplier_product_reference'],
            "product_description": row['product_description'],
            "supplier_id": supplier_id,
            "invoice_date": invoice_date,
            "year": year,
            "month": month,
            "customer_order_nb": row['customer_order_nb'],
            "member_order_nb": row['member_order_nb']
        }

        df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)

        count_import += 1
    else:
        count_reject += 1
        reject_str = ", ".join(reject_reason)
        reject_msg.append(f"{index} : {reject_str}") 

result_msg = f"imported {count_import} lines, rejected {count_reject} lines, ignored {count_member_to_member} member-to-member lines and {count_purchase} purchase lines"


# In[362]:


print(result_msg)


# In[301]:


df.head()


# In[302]:


with open(log_file, "a") as logfile:
    logfile.write(result_msg + '\r\n')
    logfile.write("\r\n".join(reject_msg) + '\r\n')


# # connect to database #

# In[303]:


service_account_file = config["google_account_auth"]
credentials = service_account.Credentials.from_service_account_file(
    service_account_file,
    scopes=["https://www.googleapis.com/auth/cloud-platform"]
)

client = bigquery.Client(credentials=credentials, project=credentials.project_id)


# In[304]:


dataset_id = config['dataset_id']
table_id = dataset_id + "." + config['turnover_table']


# ## delete existing lines with contained codes of integration ##

# In[305]:


coi_list = df['code_of_integration'].unique()

if len(coi_list) > 0:
    dml_statement = 'DELETE FROM ' + table_id + ' WHERE code_of_integration IN ('

    first = True
    for coi in coi_list:
        if not first:
            dml_statement += ', '
        dml_statement += "'" + coi + "'"
        first = False

    dml_statement += ')'

    query_job = client.query(dml_statement)
    query_job.result()


# ## upload data ##

# In[306]:


chunk_size = 1000
chunks = [df.iloc[i:i+chunk_size] for i in range(0, len(df), chunk_size)]


# In[307]:


job_config = bigquery.LoadJobConfig(
    schema=schema.schema_turnover,
    source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
)

upload_results = []
upload_errors = []

for chunk_df in chunks:
    json_data = []
    for index, row in chunk_df.iterrows():  
        json_entry = {}
        json_entry['code_of_integration'] = row['code_of_integration']
        json_entry['customer_id'] = row['customer_id']
        json_entry['plant_id'] = row['plant_id']
        json_entry['member_id'] = row['member_id']
        json_entry['member_branch_id'] = row['member_branch_id']
        json_entry['product_family_id'] = row['product_family_id']
        json_entry['quantity'] = row['quantity']
        json_entry['turnover'] = row['turnover']
        json_entry['supplier_product_reference'] = row['supplier_product_reference']
        json_entry['product_description'] = row['product_description']
        json_entry['supplier_id'] = row['supplier_id']
        json_entry['invoice_date'] = row['invoice_date']
        json_entry['year'] = row['year']
        json_entry['month'] = row['month']
        json_entry['customer_order_nb'] = row['customer_order_nb']
        json_entry['member_order_nb'] = row['member_order_nb']
        json_entry['upload_partner'] = partner
        json_entry['upload_date'] = date

        json_data.append(json_entry)

    try:
        load_job = client.load_table_from_json(
            json_data,
            table_id,
            job_config=job_config,
        )
        result = load_job.result()

        upload_results.append(f"uploaded {result.output_rows} successfully to {table_id}")
    except GoogleAPICallError as e:
        upload_errors.append(f"error: upload failed: {e}")


# In[308]:


client.close()


# check results

# In[309]:


with open(log_file, "a") as logfile:
    if len(upload_results) > 0:
        logfile.write("\r\n".join(upload_results));
    if len(upload_errors) > 0:
        logfile.write("\r\n".join(upload_errors));

    if len(upload_errors) == 0:
        result_msg += ", upload successful\r\n"
    else:
        result_msg += ", error uploading data\r\n"
    logfile.write("\r\n")
    logfile.write(result_msg)

