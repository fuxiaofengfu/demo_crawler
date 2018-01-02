#!/bin/sh
echo "start news find action"
nohup /usr/local/bin/python /home/qingniu/hainiu_cralwer/download_page/news_find_action.py > /dev/null 2>&1 &
echo "start finish..."
