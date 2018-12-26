# -*- coding: utf-8 -*-
"""
Created on Thu Apr 12 15:33:31 2018
@document:
@author: junwen
@website:http://www.data-god.com/
"""
import sys
import os,shutil
import pandas as pd
import hdfs
import time
import argparse 
import json
# =============================================================================
# This modular need  input folder exist
#  user shold put  xlsx file into input folder
#  all transfermed csv file will be in  spearate
# =============================================================================
# get current position directory

def parse_args(args):
    parser = argparse.ArgumentParser(prog="HDFS")
    parser.add_argument('configfile',type=str,help='')
    parser.add_argument("--url" , default ="hdfsurl" , type = str, help='')
    parser.add_argument("--f" , default ="hdfsurl" , type = str, help='')
    return parser.parse_args(args)

def parse(filename):
    config = open(filename)
    jsonconfig = json.load(config)
    config.close()
    return jsonconfig
# scan files in input file folder  
def transformation(directory, prefix=None, postfix=None):
    files_list = []
    for root, sub_dirs, files in os.walk(directory):
        for special_file in files:
            if postfix:
                if special_file.endswith(postfix):
                    files_list.append(os.path.join(root, special_file))
            elif prefix:
                if special_file.startswith(prefix):
                    files_list.append(os.path.join(root, special_file))
            else:
                files_list.append(os.path.join(root, special_file))
    
    # check the folder if it exists 
    folder_path = current_position + "/csv"
    folder = os.path.exists(folder_path)
    if not folder:
        os.mkdir(folder_path)
        
    for i in files_list: 
        if '.xlsx'  in i:
                print("transforming  ("+i+") into CSV")
                data_xls = pd.read_excel(i, index_col=0)
                new_files = i.replace('.xlsx', '.csv')
                data_xls.to_csv(new_files, encoding='utf-8')
                # fpath,fname=os.path.split(new_files)
                try:
                    shutil.move(new_files,folder_path)
                except:
                    pass

def loaders(url,hdfsdir,directory, path="/project",prefix=None, postfix=None):
    files_list = []
    
    if path=='/':
        print('文件路径错误')
    
    
    
    client = hdfs.Client(url,root=hdfsdir)
    for root, sub_dirs, files in os.walk(directory):
        for special_file in files:
            if postfix:
                if special_file.endswith(postfix):
                    files_list.append(os.path.join(root, special_file))
            elif prefix:
                if special_file.startswith(prefix):
                    files_list.append(os.path.join(root, special_file))
            else:
                files_list.append(os.path.join(root, special_file))
    
    # 添加检测文件夹是否存在
    ls = client.list('/')
    
    if path[1:] in ls:
        print('检测到文件 （'+ path +'） 已经存在')
    else:
        print("正在创建文件目录： " + path)
        client.makedirs(path)
    
    ls = client.list(path+'/')
    current_position = os.getcwd()
    current_position=current_position.replace('/hdfs' , '/input')
    files_ls = []
    for i in files_list:
        files_ls.append(i.replace(current_position,'')[1:])
    
    difference = list(set(files_ls).difference(set(ls)))
    if len(difference) == 0:
        print('检测所有文件已经存在，如果更新版本，请修改文件名称 例如: XXX_1.1 ,或者删除旧文件')     
    else:
        print('存在差异文件数量'+str(len(difference)))
    # client.delete(path,recursive=True)
    # print("makedir Folder" + path)
    # client.makedirs(path)
        for i in difference:
            client = hdfs.Client(url,root=hdfsdir)
            try:
                print("正在上传文件 ( "+i+" ) into HDFS...")
                client.upload(path,i)
            except:
                continue
        print("所有文件上传成功")
    # client.delete(path,recursive=True)

def get_conf(argv):
    args = parse_args(argv[1:])  
    config = parse(args.configfile)
    info = config[args.url]
    
    url = info['url']
    hdfsdir=info['dir']
    fd = info['fd']
    return url,hdfsdir,fd
    
if __name__ == '__main__':
    current_position = os.getcwd()
    print(current_position)
    url,hdfsdir,path = get_conf(sys.argv)
    current_position=current_position.replace('/hdfs' , '/input')
    transformation(current_position)
    current_position = os.getcwd()
    current_position=current_position.replace('/hdfs' , '/input')    
    os.chdir(current_position)
    loaders(url,hdfsdir,current_position,path)
