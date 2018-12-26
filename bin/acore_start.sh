nohup /root/anaconda3/envs/py3/bin/python manage.py runserver 0.0.0.0:8000 > /home/AnalysisCore/log/django_log/out.file 2>&1 &
nohup /root/anaconda3/envs/py3/bin/python -m celery -A DataEngine worker -l info > /home/AnalysisCore/log/celery_log/out_celery.file 2>&1 &
nohup /root/anaconda3/envs/py3/bin/python -m celery -A DataEngine beat -l info > /home/AnalysisCore/log/celery_log/out_beat.file 2>&1 &
echo "DataEngine server Start..."
echo "Please cheack Log file at /home/AnalysisCore/log/"