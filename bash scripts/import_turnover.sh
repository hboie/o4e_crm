#!/bin/bash

script_dir=/home/agent/scripts
python=/home/agent/o4e/bin/python
import_turnover=import_turnover.py
send_mail=send_mail.py
insert_uploadlog=insert_uploadlog.py

data_dir=/data/sftp
base_dir=upload
log_dir=log
turnover_dir=turnover
backup_dir=backup
partner_config=partner_config.json

cd $script_dir

for partner in $(ls $data_dir); do
    cur_date=`date +"%Y-%m-%d %H:%M:%S"`

    echo "search $data_dir/$partner/$base_dir/$turnover_dir"

    partner_base=$data_dir/$partner/$base_dir

    find "$partner_base/$turnover_dir" -maxdepth 1 -name '*.csv' -print0 |
    while IFS= read -r -d '' file; do
	echo "import file $file"
	logfile=$partner_base/$log_dir/`date +"%Y-%m-%d_%H%M%S_%3N"`_$partner.log

	echo "$python $import_turnover $file $logfile $partner $cur_date"
	$python $import_turnover "$file" $logfile $partner "$cur_date"

	chown :$partner $logfile
	mv "$file" $partner_base/$turnover_dir/$backup_dir
	
	import_msg=`tail -n 1 $logfile`

	echo "$python $send_mail $partner_base/$partner_config $import_msg $logfile"
	$python $send_mail $partner_base/$partner_config "$import_msg" $logfile

	$python $insert_uploadlog $partner "$import_msg" "$cur_date"
    done
done
