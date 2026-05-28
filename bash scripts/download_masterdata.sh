#!/bin/bash

cd /home/agent/scripts

/home/agent/o4e/bin/python download_masterdata.py

/home/agent/o4e/bin/python download_template.py
