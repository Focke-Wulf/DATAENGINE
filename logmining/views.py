from django.shortcuts import render

# Create your views here.
import os
import datetime
# import configParser

# serverdict,ssh_dict = configParser.configSettings('/root/DATAENGINE/conf/log_conf/guoyun_log_setting.xml')

def hello_log():
    # for server in serverdict:
    #     try:
    #         mk_cmd = 'mkdir -p /root/guoyun/' + server
    #         mk = os.popen(mk_cmd).read()
    #     except:
    #         print(mk)
    #     rs = os.popen(serverdict[server]).read()
    #     logFiles = rs.split('\n')
    #     logFiles.remove('')
    #     for log in logFiles:
    #         cmd = ssh_dict[server]+'{date}'.format(date=log)+ ' /root/guoyun/{file}'.format(file=server)
    #         rs1 = os.popen(cmd).read() 
    #         print('UPDATING...LOG_FILE_FROM_REMOTE SERVER  : [ '+ server  +' ]  filename = '+log)
    #         print(rs1)
    pass

