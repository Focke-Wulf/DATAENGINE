from django.shortcuts import render

# Create your views here.
import os
import datetime

serverdict = { 
            'moojnn_school_3' : "ssh -p 1122 root@121.40.229.164 'cd /guoyun/apache-tomcat-8.0.24/logs ; ls|grep localhost_access_log'",
            'moojnn_school_4' : "ssh -p 1122 root@121.40.219.124 'cd /guoyun/apache-tomcat-8.0.24/logs ; ls|grep localhost_access_log'",
            'moojnn_school_5' : "ssh -p 1122 root@121.40.237.93 'cd /guoyun/apache-tomcat-8.0.24/logs ; ls|grep localhost_access_log'",
            'moojnn_school_6' : "ssh -p 1122 root@121.43.99.178 'cd /guoyun/apache-tomcat-8.0.24/logs ; ls|grep localhost_access_log'",
            'mengniu_mojinServer_pro' : "ssh root@120.26.83.147 'cd /moojnn/data/logs ; ls|grep mojing-user.log'",# moojnnServer for mengniu
            'moojing_server_pro2' : "ssh root@120.26.221.2 'cd /moojnn/data/logs ; ls|grep mojing-user.log'", # moojnn_server_pro2
            'moojnn_school_csp_pro_4' : "ssh root@120.26.101.233 'cd /data/mojingcsp/logs ; ls|grep localhost_access_log'", # moojnn-csp-pro-4 for School
            'moojnn_school_csp_pro_4_base' : "ssh root@120.26.101.233 'cd /data/mojingcsp/logs/mojing_csp_log ; ls|grep base'", # moojnn-csp-pro-4 for School
            'moojnn_school_csp_pro_4_dao' : "ssh root@120.26.101.233 'cd /data/mojingcsp/logs/mojing_csp_log ; ls|grep base'", # moojnn-csp-pro-4 for School
            'moojnn_school_csp_pro_4_communication' : "ssh root@120.26.101.233 'cd /data/mojingcsp/logs/mojing_csp_log ; ls|grep base'", # moojnn-csp-pro-4 for School
            'moojnn_school_csp_pro_4_task' : "ssh root@120.26.101.233 'cd /data/mojingcsp/logs/mojing_csp_log ; ls|grep task'", # moojnn-csp-pro-4 for School
            'moojnn_school_csp_pro_3' : "ssh root@120.26.73.36 'cd /data/mojingcsp/logs ; ls|grep localhost_access_log'", # moojnn-csp-pro-3 for School
            'moojnn_school_csp_pro_3_base' : "ssh root@120.26.73.36 'cd /data/mojingcsp/logs/mojing_csp_log ; ls|grep base'", # moojnn-csp-pro-3 for School
            'moojnn_school_csp_pro_3_dao' : "ssh root@120.26.73.36 'cd /data/mojingcsp/logs/mojing_csp_log ; ls|grep base'", # moojnn-csp-pro-3 for School
            'moojnn_school_csp_pro_3_communication' : "ssh root@120.26.73.36 'cd /data/mojingcsp/logs/mojing_csp_log ; ls|grep base'", # moojnn-csp-pro-3 for School
            'moojnn_school_csp_pro_3_task' : "ssh root@120.26.73.36 'cd /data/mojingcsp/logs/mojing_csp_log ; ls|grep task'", # moojnn-csp-pro-3 for School
}

ssh_dict = {
            'moojnn_school_3' : "scp -P 1122 root@121.40.229.164:/guoyun/apache-tomcat-8.0.24/logs/",
            'moojnn_school_4' : "scp -P 1122 root@121.40.219.124:/guoyun/apache-tomcat-8.0.24/logs/",
            'moojnn_school_5' : "scp -P 1122 root@121.40.237.93:/guoyun/apache-tomcat-8.0.24/logs/",
            'moojnn_school_6' : "scp -P 1122 root@121.43.99.178:/guoyun/apache-tomcat-8.0.24/logs/",
            'mengniu_mojinServer_pro' : "scp -P 22 root@120.26.83.147:/moojnn/data/logs/",# moojnnServer for mengniu
            'moojing_server_pro2' : "scp -P 22 root@120.26.221.2:/moojnn/data/logs/", # moojnn_server_pro2
            'moojnn_school_csp_pro_4' : "scp -P 22 root@120.26.101.233:/data/mojingcsp/logs/", # moojnn-csp-pro-4 for School
            'moojnn_school_csp_pro_4_base' : "scp -P 22 root@120.26.101.233:/data/mojingcsp/logs/mojing_csp_log/", # moojnn-csp-pro-4 for School
            'moojnn_school_csp_pro_4_dao' : "scp -P 22root@120.26.101.233:/data/mojingcsp/logs/mojing_csp_log/", # moojnn-csp-pro-4 for School
            'moojnn_school_csp_pro_4_communication' : "scp -P 22 root@120.26.101.233:/data/mojingcsp/logs/mojing_csp_log/", # moojnn-csp-pro-4 for School
            'moojnn_school_csp_pro_4_task' : "scp -P 22 root@120.26.101.233:/data/mojingcsp/logs/mojing_csp_log/", # moojnn-csp-pro-4 for School
            'moojnn_school_csp_pro_3' : "scp -P 22 root@120.26.73.36:/data/mojingcsp/logs/", # moojnn-csp-pro-3 for School
            'moojnn_school_csp_pro_3_base' : "scp -P 22 root@120.26.73.36:/data/mojingcsp/logs/mojing_csp_log/", # moojnn-csp-pro-3 for School
            'moojnn_school_csp_pro_3_dao' : "scp -P 22 root@120.26.73.36:/data/mojingcsp/logs/mojing_csp_log/", # moojnn-csp-pro-3 for School
            'moojnn_school_csp_pro_3_communication' : "scp -P 22 root@120.26.73.36:/data/mojingcsp/logs/mojing_csp_log/", # moojnn-csp-pro-3 for School
            'moojnn_school_csp_pro_3_task' : "scp -P 22 root@120.26.73.36:/data/mojingcsp/logs/mojing_csp_log/", # moojnn-csp-pro-3 for School


}

def hello_log():
    for server in serverdict:
        try:
            mk_cmd = 'mkdir -p /root/guoyun/' + server
            mk = os.popen(mk_cmd).read()
        except:
            print(mk)
        rs = os.popen(serverdict[server]).read()
        logFiles = rs.split('\n')
        logFiles.remove('')
        for log in logFiles:
            cmd = ssh_dict[server]+'{date}'.format(date=log)+ ' /root/guoyun/{file}'.format(file=server)
            rs1 = os.popen(cmd).read() 
            print('UPDATING...LOG_FILE_FROM_REMOTE SERVER  : [ '+ server  +' ]  filename = '+log)
            print(rs1)

hello_log()