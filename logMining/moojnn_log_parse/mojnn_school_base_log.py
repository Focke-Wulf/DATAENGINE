import re
import pandas as pd
import time
import os

 
logRunerConf = {
    'moojnn_school_csp_pro_3_base':['moojnn_school_csp_pro_3_base','moojnn_school_csp_pro_4_base'
                                   ],
}

def parser_base_log(log):
    row = list()
    column = list()
    with open(log,'r') as f:
        for i in f.readlines():
            x = i.split('\t')
            try:
                date = x[0].split(' ')[0] 
                datestru = time.strptime(date,"%Y-%m-%d")
                year = datestru.tm_year #0
                month = datestru.tm_mon #1
                day = datestru.tm_mday #2
                timez = x[0].split(' ')[1] #3
                intercepter = x[0].split(' ')[2].split('-')[0].lstrip('[').rstrip(']') #4
                logLevel = x[0].split(' ')[3] #5
                log_methods = x[0].split(' ')[4]
                tracking_id = x[0].split(' ')[6].split(']')[0].lstrip('[')
                http_methods = x[0].split(' ')[6].split(']')[1].split(':')[0] #6
                api = x[0].split(' ')[6].split(']')[1].split(':')[1]  #7
                #gn = re.search('(?<=GET:).*|(?<=POST:).*|(?<=DELETE:).*',gongneng).group(0).split('/')
                remoteHost = x[1] #8
                remoteIP = x[2] #9
                webaddress = x[3] #11
                system = x[4] #12
                browser_CORE = x[5] #13
                browser = x[6] #14
                cookie = x[7] #15
                email = x[8] #16
                user_id = x[9] #17
                moojnn_version = x[10] #18
                license = x[11] #19
                action = x[12].replace("'","\"") #20
                row = (year,month,day,timez,intercepter,logLevel,log_methods,tracking_id,http_methods,api,remoteHost,remoteIP,webaddress,system,browser_CORE,browser,cookie,email,user_id,moojnn_version,license,action)
                column.append(row)
            except:
                print("ERROR :",x)
                continue
    f.close()
    return column


def pars_runer():
    from postgresql import pg2connector
    pg2 = pg2connector.PostgresData()
    
    for value in logRunerConf:
        header_name = ['year','month','day','timez','intercepter','logLevel','log_methods','tracking_id','http_methods','api','remoteHost','remoteIP','webaddress','system','browser_CORE','browser','cookie','email','user_id','moojnn_version','license','action']
        header_type = ['text','text','text','text','text','text','text','text','text','text','text','text','text','text','text','text','text','text','text','text','text','text']
        try:
            pg2.create_table(value,header_name,header_type)
        except Exception as e1:
            print("Create_table :",e1)
            continue
        for logs in logRunerConf[value]:
            log_list = os.listdir('/root/guoyun/{log}'.format(log=logs))
            for i in log_list:
                loc = '/root/guoyun/{log}/{file}'.format(log=logs,file=i)
                row_list = parser_base_log(loc)
                for row in row_list:
                    try:
                        pg2.insert(value,header_name,header_type,row)
                    except Exception as e2:
                        print("Inster_table :",e2)
                        continue


if __name__ == "__main__":
    
    pars_runer()