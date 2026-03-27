#!/usr/bin/env python
# coding: utf-8

# # send reminder #

# In[52]:


import json
import re
import sys
from datetime import datetime, timedelta
from google.cloud import bigquery
from google.oauth2 import service_account
from google.api_core.exceptions import GoogleAPICallError
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart


# ## import configuration ##

# In[53]:


environment = 'prod'
try:
    with open("env") as f:
        environment = f.read()
finally:
    pass

environment


# In[54]:


if environment == 'test':
    config_file = "configuration-test.json"
else:
    config_file = "configuration.json"

with open(config_file) as f:
    config = json.load(f)

config_file


# In[55]:


partner = 'boie'
partner_config_file = 'partner_config.json'

if re.match('send_reminder.py', sys.argv[0]):
    if len(sys.argv) >= 1:
        partner = sys.argv[1]
    if len(sys.argv) >= 2:
        partner_config_file = sys.argv[2]


# # connect to database #

# In[56]:


service_account_file = config["google_account_auth"]
credentials = service_account.Credentials.from_service_account_file(
    service_account_file,
    scopes=["https://www.googleapis.com/auth/cloud-platform"]
)

client = bigquery.Client(credentials=credentials, project=credentials.project_id)


# In[57]:


dataset_id = config['dataset_id']
table_id = dataset_id + "." + config['turnover_table']


# In[58]:


ref_date = datetime.today().replace(day=1, hour=0, minute=0, second=0, microsecond=0)
ref_date = ref_date - timedelta(days=1)
ref_date = ref_date.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

year = ref_date.year
month = ref_date.strftime('%B')   


# In[59]:


dml_statement = 'SELECT count(*) FROM ' + table_id + ' WHERE upload_partner=@partner and invoice_date > @ref_date'

job_config = bigquery.QueryJobConfig(
    query_parameters=[bigquery.ScalarQueryParameter("partner", "STRING", partner), 
                      bigquery.ScalarQueryParameter("ref_date", "DATETIME", ref_date)]
)

query_job = client.query(dml_statement, job_config=job_config)
result = query_job.result()

first_row = next(result, None)
count = first_row[0]

print(f"number of entries: {count}")


# In[ ]:


client.close()


# ## send email ##

# In[49]:


with open(partner_config_file) as f:
    partner_config = json.load(f)

mail_to = partner_config['email']


# In[60]:


smtp_server = config['smtp_server']
smtp_port = config['smtp_port']
username = config['username']
password = config['password']

subject = f"ONE reporting for {month} {year} for partner {partner}"
body = f"There are no entries with invoice-dates from {month} {year} in the ONE CRM database for partner {partner}.\n"
body += f"Please check if the data reporting is up and working. All data should be reported by the 10th of the following month."


# In[61]:


if count == 0:
    msg = MIMEMultipart()
    msg['From'] = config['email_from']
    msg['To'] = mail_to
    msg['Subject'] = subject

    if 'email_cc' in config:
        msg['Cc'] = config['email_cc']

    msg.attach(MIMEText(body, 'plain'))

    try:
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.login(username, password)
            server.send_message(msg)
            print("email sent successfully!")
    except Exception as e:
        print("error while sending email:", e)
else:
    print("valid data found - nothing to do");


# In[ ]:




