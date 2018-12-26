ps -ef|grep DataEngine |grep -v grep|cut -c 9-15|xargs kill -9
ps -ef|grep celery |grep -v grep|cut -c 9-15|xargs kill -9
lsof -i:8000 | grep 8000 | awk -F' ' '{print $2}'|xargs kill -9
echo "DataEngine Server has Stoped..."