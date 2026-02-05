#!/usr/bin/env python
# coding: utf-8

# ## import ##

# In[2]:


import json
import re
import sys
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart


# ## read parameter ##

# In[3]:


partner_config_file = 'partner_config.json'
subject = 'Test E-Mail'
body_file = 'body.txt'

if re.match('send_mail.py', sys.argv[0]):
    if len(sys.argv) >= 1:
        partner_config_file = sys.argv[1]
    if len(sys.argv) >= 2:
        subject = sys.argv[2]
    if len(sys.argv) >= 3:
        body_file = sys.argv[3]

with open(body_file) as f:
    body = f.read()


# ## import configuration ##

# In[4]:


environment = 'prod'
try:
    with open("env") as f:
        environment = f.read()
finally:
    pass

environment


# In[5]:


if environment == 'test':
    config_file = "configuration-test.json"
else:
    config_file = "configuration.json"

with open(config_file) as f:
    config = json.load(f)


# In[6]:


smtp_server = config['smtp_server']
smtp_port = config['smtp_port']
username = config['username']
password = config['password']


# ## load partner configuration ##

# In[7]:


with open(partner_config_file) as f:
    partner_config = json.load(f)

mail_to = partner_config['email']


# ## send email ##

# In[8]:


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
        print("E-Mail erfolgreich gesendet!")
except Exception as e:
    print("Fehler beim Senden:", e)


# In[ ]:




