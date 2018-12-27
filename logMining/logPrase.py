import re
import pandas as pd
import time
def parser_base_log(log):
    a = ''
    row = list()
    column = list()
    with open(log,'r') as f:
        for i in f.readlines():
            x = i.split('\t')
            date = x[0].split(' ')[0] #0
            time = x[0].split(' ')[1] #1
            gongneng = x[0].split(' ')[6]
            gn = re.search('(?<=GET:).*',gongneng).group(0).split('/')
            gn.remove('')
            gn.remove('mojingcsp')
            gn.remove('public')
            for i in gn:
                a = a+i+','
            action = a.strip(',') #2
            a = ''
            remoteIP = x[1] #3
            remoteHost = x[2] #4
            webaddress = x[3] #5
            system = x[4].split(';')[0]
            browser = x[6]
            row = [date,time,remoteIP,remoteHost,webaddress,system,browser,action]
            column.append(row)
    f.close()
    return column
# def parser_communication_log():

def parser_mojing_user_log():
    a = ''
    row = list()
    column = list()
    with open('/root/guoyun/moojnn_school_3/localhost_access_log.2018-12-18.txt','r') as f:
        for i in f.readlines():
            x = i.split('\t')
            date = x[0].split(' ')[0] #0
            time = x[0].split(' ')[1] #1
            gongneng = x[0].split(' ')[4]
            gn = re.search('(?<=GET:).*|(?<=POST:).*|(?<=DELETE:).*',gongneng).group(0).split('/')
            gn.remove('')
            gn.remove('mojing-server')
            for i in gn:
                a = a+i+','
            action = a.strip(',') #2
            a = ''
            remoteIP = x[1] #3
            remoteHost = x[2] #4
            webaddress = x[3] #5
            system = x[4].split(';')[0]
            browser = x[6]
            row = [date,time,remoteIP,remoteHost,webaddress,system,browser,action]
            column.append(row)
    f.close()
    return column

def parser_access_log(log):
    file = log
    row = list()
    column = list()
    with open(file,'r') as f:
        for i in f.readlines():
            x = i.split(' ')
            ipaddress = x[0] #0
            datestru = time.strptime(x[1].lstrip('[').split(':',1)[0],"%d/%b/%Y")
            year = datestru.tm_year #1
            month = datestru.tm_mon #2
            day = datestru.tm_mday #3
            timezone = x[2].rstrip(']') #4
            tpr = x[3] #5
            reqest_api = x[4] #6
            host = x[5] #7
            http_headers = x[6].lstrip('"') #8
            reqest_api2 = x[7] #9
            Http_version = x[8].rstrip('"') #10
            status = x[9].strip('\n') #11
            row = [ipaddress,year,month,day,timezone,tpr,reqest_api,host,http_headers,reqest_api2,Http_version,status]
            print(row)
            column.append(row)
    f.close()
    return column
# x = parser_base_log('./2018-12-15.base.log')
x = parser_access_log('/root/guoyun/moojnn_school_3/localhost_access_log.2018-12-18.txt')
print(x)

 

 