from xml.dom.minidom import parse
import xml.dom.minidom 
import os

def configSettings(xml_file_path):
    tree = parse(xml_file_path)
    config = tree.documentElement
    ts = config.getElementsByTagName('property')
    spark_configuation = ''
    yarn_configuation = ''
    script_configuation = ''
    for i in ts:
        if i.hasAttribute("spark"):
            name = i.getElementsByTagName('name')[0].childNodes[0].data
            value = i.getElementsByTagName('value')[0].childNodes[0].data
            if name == 'spark-submit':
                spark_configuation = spark_configuation+value+name
            else:
                spark_configuation = spark_configuation +' '+ name +' '+ value
        elif i.hasAttribute("yarn"):
            name = i.getElementsByTagName('name')[0].childNodes[0].data
            if name == 'yarn.rest.url':
                yarn_configuation = i.getElementsByTagName('value')[0].childNodes[0].data
        elif i.hasAttribute("script"):
            name = i.getElementsByTagName('name')[0].childNodes[0].data
            if name == 'scriptFileLocation':
                script_configuation = i.getElementsByTagName('value')[0].childNodes[0].data
    

    return spark_configuation,yarn_configuation,script_configuation
