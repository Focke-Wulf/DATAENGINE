from django.shortcuts import render
from django.http import HttpResponse,JsonResponse
from django.views.decorators.csrf import csrf_exempt
from rest_framework.renderers import JSONRenderer
from rest_framework.parsers import JSONParser
from rest_framework.response import Response
from rest_framework.decorators import api_view
from rest_framework import status
__doc__="""
           \033[4;33m\t======= DataEngine Activate ======\033[0m
    _____         _______         _____  __    __  _____
    |  _ \\       \_______\\       ||____| ||\\\\  || ||____|
    | | | \\  ___    | |   ___    ||____  || \\\\ || ||____
    | |_| / / __ \\  | |  / __ \\  ||____| ||  \\\\|| ||____|
    |____/  \\_,_/ \\ | |  \\_,_/ \\ ||____  ||   \\_| ||____|

      ____              __
     / __/__  ___ _____/ /__   \033[4;34m\t® DataEngine\033[0m
    _\ \/ _ \/ _ `/ __/  '_/
   /__ / .__/\_,_/_/ /_/\_\   \033[4;31m\tspark 2.3.1.3.0.0.0-1634\033[0m
      /_/                     \033[4;32m\tDataEngine alpha v 0.1\033[0m
            """

from spark_api.models import Execute
from spark_api.serializers import ExecuteSerializer
from . import tasks
import traceback
import time,datetime
import os
from spark_api.parseConf import configSettings
import json
print(__doc__)
import logging
LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
pwd = os.getcwd()
# father_path=os.path.abspath(os.path.dirname(pwd)+os.path.sep+".")
# TODO(@junwen) 这里的环境变量最好不要设定成相对路径，希望能直接从系统环境变量里提取文件位置
conf_path = pwd + '/conf/local/engine-site.xml'
sub_cmd = ""
script_conf = ""
yarn_rest_api = ""
# -------------------------------------------------------
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


@api_view(['GET', 'POST'])
def execute(request):
    if request.method == 'POST':
        serializer = ExecuteSerializer(data=request.data) 
        if serializer.is_valid():
            serializer.save()
            JOB_ID = serializer.data["job_id"]
            SCRIPT = serializer.data["script"]
            LANGUAGE_TYPE = serializer.data["type"]
            SYNC = serializer.data["sync"]
            if LANGUAGE_TYPE == "python":
                try:
                    # submmit_mission_time = datetime.datetime.now()
                    # submmit_mission_time.strftime("%Y-%m-%d %H:%M:%S")
                    tasks.sparkSubmit.delay(SCRIPT,JOB_ID)
                    result_dict = {'script_file_name':JOB_ID,'success':'True','job_id':JOB_ID,'type':LANGUAGE_TYPE}
                    return Response(result_dict,status=status.HTTP_201_CREATED)
                except Exception:
                    error = traceback.format_exc()
                    content = {'success':'false','error':error}
                    return Response(content, status=status.HTTP_400_BAD_REQUEST)
            else:
                result_dict = {'script_file_name':JOB_ID,'False':'True','job_id':JOB_ID,'type':LANGUAGE_TYPE}
                return Response(result_dict,status=status.HTTP_201_CREATED)
        else:
            return Response({'Error':'job_id has already exist'},status=status.HTTP_201_CREATED)

    else:
        return Response({'Error':'Only support POST methods'},status=status.HTTP_201_CREATED)

@api_view(['GET', 'POST'])
def log(request):
    if request.method == 'POST':
        serializer = ExecuteSerializer(data=request.data)
        if serializer.is_valid():serializer.save()
        JOB_ID = serializer.data["job_id"]  
        from urllib import parse
        from urllib import request as qq
        log = ''
        try:
            obj = Execute.objects.get(job_id=str(JOB_ID))
            app_id = obj.__dict__['appid']
            log = obj.__dict__['log']
            print('当前执行查询任务为: ' + app_id)
            print('===============Print==============')
            print(log)
            url = yarn_rest_api + app_id
            req = qq.Request(url)
            res_data = qq.urlopen(req) 
            res = res_data.read()
            jo = json.loads(res)
            content = jo['app']
            content['log'] = log
            jx = log.split(' ')
            scriptSuccess = False
            if 'error' in jx:
                scriptSuccess = False
            else:
                scriptSuccess = True
            content['scriptSuccess'] = scriptSuccess  
            return Response(content,status=status.HTTP_201_CREATED)
        except:
            if len(log) == 0:
                result_dict = {'script_file_name':JOB_ID,'state':'PENDING','job_id':JOB_ID,'log':log}
            else:
                result_dict = {'script_file_name':JOB_ID,'state':'ERROR','job_id':JOB_ID,'log':log}
            return Response(result_dict,status=status.HTTP_201_CREATED)
    else:
        return Response({'Error':'Only support POST methods'},status=status.HTTP_201_CREATED)


@api_view(['GET', 'POST'])    
def kill(request):
    cmd = 'yarn application -kill '
    if request.method == 'POST':
        serializer = ExecuteSerializer(data=request.data)
        if serializer.is_valid():
            JOB_ID = serializer.data["job_id"]
            cmdapp = cmd + JOB_ID
            rs = os.popen(cmdapp)
            content = {'job_id':JOB_ID,'state':'killed','messages':rs}
            return Response(content,status=status.HTTP_201_CREATED)
        else:
            content = {'job_id':JOB_ID,'state':'Error','messages':'error'}
            return Response(content,status=status.HTTP_201_CREATED)
    else:
        return Response({'Error':'Only support POST methods'},status=status.HTTP_201_CREATED)

            