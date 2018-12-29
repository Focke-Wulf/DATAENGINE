from __future__ import absolute_import, unicode_literals
from celery import app
from celery import shared_task
import os,django
import traceback
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "DataEngine.settings")# project_name 项目名称
django.setup()
import json
import time,datetime
from spark_api.models import Execute
from urllib import parse,request
from spark_api.parseConf import configSettings
import logging
LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
# -------------------------------------------------------
pwd = os.getcwd()
# father_path=os.path.abspath(os.path.dirname(pwd)+os.path.sep+".")
# TODO(@junwen) 这里的环境变量最好不要设定成相对路径，希望能直接从系统环境变量里提取文件位置
conf_path = pwd + '/conf/local/engine-site.xml'
# -------------------------------------------------------
sub_cmd = ""
script_conf = ""
yarn_rest_api = ""
try:
    sub_cmd,yarn_rest_api,script_conf = configSettings(conf_path)
    if sub_cmd == '':
        logging.error('Not a vaild spark-submit setting, please check engine-site.xml')
    elif yarn_rest_api == '':
        logging.error('Not a vaild yarn setting, please check engine-site.xml')
    elif script_conf == '':
        logging.error('Not a vaild script path setting, please check engine-site.xml')
except:
    logging.error('Can not read the config setting engine-site.xml may not exit')

logging.info('spark setting: '+sub_cmd)
logging.info('script location: '+script_conf)
logging.info('yarn setting: '+yarn_rest_api)


@shared_task
def yarn_loginfo(jobid=None,appName=None,submit_time=None):
    command = "yarn application -list | grep application | awk -F'\t' '{print $1}'"
    val = os.popen(command).read()
    appid = val.split("\n")
    appid.remove(appid[0])
    appid.remove('')
    print(appid)
    for i in appid:
        url = yarn_rest_api+i
        req = request.Request(url)
        res_data = request.urlopen(req) 
        res = res_data.read()
        jo = json.loads(res)
        misstion_start_time = int(jo["app"]["startedTime"])
        misstion_start_time = int(misstion_start_time* (10 ** (10-len(str(misstion_start_time)))))
        misstion_start_time = datetime.datetime.fromtimestamp(misstion_start_time)
        print("时间差: 上传时间" + str(submit_time) +" yarn 任务开始时间 " + str(misstion_start_time))
        if appName == jo["app"]["name"]:
            Execute.objects.filter(job_id=str(jobid)).update(appid=str(i))
        

@shared_task
def sparkSubmit(script,jobId):
    job_id = ''
    job_id = jobId 

    script_file_name = job_id+'.py'
    script_path = ' '+ script_conf +script_file_name
    
    script_file_path = str(script_conf) + script_file_name
    
    with open(script_file_path,'w') as f:
        f.write(script)
        f.close()
    global sub_cmd
    sub_cmd = sub_cmd +' --name ' +jobId
    try:
       cmd = sub_cmd + script_path
       print('上传命令 : ' +cmd)
       t_s = os.popen(cmd).read()
       content={'msg':t_s}
       Execute.objects.filter(job_id=str(job_id)).update(log=str(t_s))
       return content
    except Exception:
       error = traceback.format_exc()
       content = {'success':'false','error':error}
       print(content)
       return content
       

@shared_task
def get_log_from_spark():
    command = "yarn application -list | grep application | awk -F'\t' '{print $1}'"
    val = os.popen(command).read()
    appid = val.split("\n")
    appid.remove(appid[0])
    appid.remove('')
    print(appid)
    for i in appid:
        url = yarn_rest_api+i
        req = request.Request(url)
        res_data = request.urlopen(req) 
        res = res_data.read()
        jo = json.loads(res)
        appName = jo["app"]["name"]
        print(appName)
        # print(appName)
        # titleList = Execute.objects.filter(title=str(appName))
        # if len(titleList)>0:
        #     for obj in titleList:
        #         obj.update(appid=str(i))
        Execute.objects.filter(job_id=str(appName)).update(appid=str(i))


