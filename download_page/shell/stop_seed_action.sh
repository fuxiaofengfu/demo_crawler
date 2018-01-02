#!/bin/sh
echo "stop news find action"
PID="`ps -ef|grep python|grep news_find_action|grep -v grep|awk '{print $2}'`"
kill ${PID}
echo "stop finish..."