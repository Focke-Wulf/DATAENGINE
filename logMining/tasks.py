from __future__ import absolute_import, unicode_literals
from celery import app
from celery import shared_task
import os,django
import traceback
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "DataEngine.settings")# project_name 项目名称
django.setup()
import json
import time,datetime
from logMining.models import Logrecord
from urllib import parse,request
from spark_api.parseConf import configSettings
import logging


# check file list
log_file_ = {
    '/root/guoyun/' :['mengniu_mojinServer_pro','moojnn_school_3','moojnn_school_4','moojnn_school_5','moojnn_school_6','moojnn_school_csp_pro_3','moojnn_school_csp_pro_3_task','moojnn_school_csp_pro_4_base','moojnn_school_csp_pro_4_dao',
         'moojing_server_pro2','moojnn_school_3_user','moojnn_school_4_user','moojnn_school_5_user','moojnn_school_6_user','moojnn_school_csp_pro_3_base','moojnn_school_csp_pro_4','moojnn_school_csp_pro_4_communication','moojnn_school_csp_pro_4_task']
}


@shared_task
def log_checker():
    print('a')