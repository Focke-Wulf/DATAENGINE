import re
import pandas as pd
import time
import os

logRunerConf = {
    'moojnn_school':['moojnn_school_3',
                    'moojnn_school_4',
                    'moojnn_school_5',
                    'moojnn_school_6',
                    'moojnn_school_csp_pro_3',
                    'moojnn_school_csp_pro_4'],
}

def parser_access_log(log):
    file = log
    row = list()
    column = list()
    with open(file,'r') as f:
        for i in f.readlines():
            x = i.split(' ')
            try:
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
                column.append(row)
            except:
                print(x)
                continue
    f.close()
    return column

def pars_runer():
    from postgresql import pg2connector
    pg2 = pg2connector.PostgresData()
    
    for value in logRunerConf:
        header_name = ['ipaddress','year','month','day','timezone','tpr','reqest_api','host','http_headers','reqest_api2','Http_version','status']
        header_type = ['text','text','text','text','text','text','text','text','text','text','text','text']
        try:
            pg2.create_table(value,header_name,header_type)
        except:
            pass
        for logs in logRunerConf[value]:
            log_list = os.listdir('/root/guoyun/{log}'.format(log=logs))
            for i in log_list:
                loc = '/root/guoyun/{log}/{file}'.format(log=logs,file=i)
                print("RUNNING Parser tbale :"+ logs)
                row_list = parser_access_log(loc)
                for row in row_list:
                    pg2.insert(value,header_name,header_type,row)
                    print("INSTER LOG Result INTO TABLE ..."+value)

if __name__ == "__main__":
    pars_runer()