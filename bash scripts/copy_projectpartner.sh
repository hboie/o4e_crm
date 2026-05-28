#!/bin/bash

template="data/ONE CRM reporting template.xlsx"
project="sefi efrapo polarbearings"

create_project_partner_crm_db=create_project_partner_crm_db.py
python=/home/agent/o4e/bin/python
data_dir=/data/sftp
base_dir=upload
template_dir=template

cd /home/agent/scripts

for partner in $project; do
    cur_date=`date +"%Y-%m-%d %H:%M:%S"`

    partner_base=$data_dir/$partner/$base_dir

    target_dir=$partner_base/$template_dir
    
    echo "copy data to $target_dir"

    if [ ! -d $target_dir ]; then
	echo "create directory $target_dir"
	mkdir $target_dir
    fi

    cp "$template" $target_dir

    $python $create_project_partner_crm_db $partner $partner_base $template_dir
done

