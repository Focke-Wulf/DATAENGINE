from xml.dom.minidom import parse
import xml.dom.minidom 
import os

def configSettings(xml_file_path):
    tree = parse(xml_file_path)
    config = tree.documentElement
    ts = config.getElementsByTagName('property')
    ssh_dict = {}
    scp_dict = {}
    for i in ts:
        name = i.getElementsByTagName('name')[0].childNodes[0].data
        ip_address = i.getElementsByTagName('ip_address')[0].childNodes[0].data
        port = i.getElementsByTagName('port')[0].childNodes[0].data
        system_user = i.getElementsByTagName('system_user')[0].childNodes[0].data
        log_file_path = i.getElementsByTagName('log_file_path')[0].childNodes[0].data
        filter = i.getElementsByTagName('filter')[0].childNodes[0].data
        ssh_cmd = "ssh -p {p} {su}@{ip} 'cd {path} ; ls|grep {filter}'".format(p=port,su=system_user,ip=ip_address,path=log_file_path,filter=filter)
        scp_cmd = "scp -P {p} {su}@{ip}:{path}".format(p=port,su=system_user,ip=ip_address,path=log_file_path)
        ssh_dict[name] = ssh_cmd
        scp_dict[name] = scp_cmd
    return ssh_dict,scp_dict
