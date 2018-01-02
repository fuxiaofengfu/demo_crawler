#!/bin/sh
echo "stop news find action"
PID="`ps -ef|grep python|grep download_action|grep -v grep|awk '{print $2}'`"
kill ${PID}
echo "stop finish..."
