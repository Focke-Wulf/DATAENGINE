# -*- coding: utf-8 -*-
# @Author: 谷志翔
# @Date:   2018-07-11 09:08:02
# @Last Modified by:   谷志翔
# @Last Modified time: 2018-08-01 16:45:58

import re
import datetime


class IDCARD:

    __doc__ = '''identity card check'''

    def __init__(self, idcard):
        self.idcard = str(idcard).strip()
        self.id_status = None
        self.id_area = None
        self.id_birth = None
        self.id_age = None
        self.id_sex = None

        self.Errors = ['验证通过', '身份证号码位数非法',
                       '身份证号码出生日期超出范围或含有非法字符', '身份证号码校验错误', '身份证地区非法']

        self.areas = {"11": "北京", "12": "天津", "13": "河北", "14": "山西", "15": "内蒙古", "21": "辽宁", "22": "吉林", "23": "黑龙江", "31": "上海", "32": "江苏", "33": "浙江", "34": "安徽", "35": "福建", "36": "江西", "37": "山东", "41": "河南", "42": "湖北",
                      "43": "湖南", "44": "广东", "45": "广西", "46": "海南", "50": "重庆", "51": "四川", "52": "贵州", "53": "云南", "54": "西藏", "61": "陕西", "62": "甘肃", "63": "青海", "64": "宁夏", "65": "新疆", "71": "台湾", "81": "香港", "82": "澳门", "91": "国外"}

    def _birthday(self):
        if len(self.idcard) == 15:
            birthday = datetime.date(
                int(self.idcard[6:8] + 1900), int(self.idcard[8:10]), int(self.idcard[10:12]))
        if len(self.idcard) == 18:
            birthday = datetime.date(
                int(self.idcard[6:10]), int(self.idcard[10:12]), int(self.idcard[12:14]))
        return birthday

    def _age(self):
        if len(self.idcard) == 15:
            age = datetime.datetime.now().year - (int(self.idcard[6:8]) + 1900)
        if len(self.idcard) == 18:
            age = datetime.datetime.now().year - int(self.idcard[6:10])
        return age

    def _sex(self):
        if int(self.idcard[-2]) % 2 == 0:
            return '女'
        else:
            return '男'

    def _areaCheck(self):
        if not self.areas[self.idcard[:2]]:
            self.id_status = self.Errors[4]

    def _dateCheck(self):
        if len(self.idcard) == 15:
            if ((int(self.idcard[6:8]) + 1900) % 4 == 0) or (((int(self.idcard[6:8]) + 1900) % 100 == 0) and ((int(self.idcard[6:8]) + 1900) % 4 == 0)):
                ereg = re.compile(
                    '[1-9][0-9]{5}[0-9]{2}((01|03|05|07|08|10|12)(0[1-9]|[1-2][0-9]|3[0-1])|(04|06|09|11)(0[1-9]|[1-2][0-9]|30)|02(0[1-9]|[1-2][0-9]))[0-9]{3}$')
            else:
                ereg = re.compile(
                    '[1-9][0-9]{5}[0-9]{2}((01|03|05|07|08|10|12)(0[1-9]|[1-2][0-9]|3[0-1])|(04|06|09|11)(0[1-9]|[1-2][0-9]|30)|02(0[1-9]|1[0-9]|2[0-8]))[0-9]{3}$')
            if re.match(ereg, self.idcard):
                self.id_status = self.Errors[0]
            else:
                self.id_status = self.Errors[2]
        elif len(self.idcard) == 18:
            if (int(self.idcard[6:10]) % 4 == 0) or ((int(self.idcard[6:10]) % 100 == 0) and (int(self.idcard[6:10]) % 4 == 0)):
                ereg = re.compile(
                    '[1-9][0-9]{5}19[0-9]{2}((01|03|05|07|08|10|12)(0[1-9]|[1-2][0-9]|3[0-1])|(04|06|09|11)(0[1-9]|[1-2][0-9]|30)|02(0[1-9]|[1-2][0-9]))[0-9]{3}[0-9Xx]$')
            else:
                ereg = re.compile(
                    '[1-9][0-9]{5}19[0-9]{2}((01|03|05|07|08|10|12)(0[1-9]|[1-2][0-9]|3[0-1])|(04|06|09|11)(0[1-9]|[1-2][0-9]|30)|02(0[1-9]|1[0-9]|2[0-8]))[0-9]{3}[0-9Xx]$')
            if re.match(ereg, self.idcard):
                self._parityCheck()
            else:
                self.id_status = self.Errors[2]
        else:
            self.id_status = self.Errors[1]

    def _parityCheck(self):
        id_list = list(self.idcard)
        S = (int(id_list[0]) + int(id_list[10])) * 7 + (int(id_list[1]) + int(id_list[11])) * 9 + (int(id_list[2]) + int(id_list[12])) * 10 + (int(id_list[3]) + int(id_list[13])) * 5 + (int(
            id_list[4]) + int(id_list[14])) * 8 + (int(id_list[5]) + int(id_list[15])) * 4 + (int(id_list[6]) + int(id_list[16])) * 2 + int(id_list[7]) * 1 + int(id_list[8]) * 6 + int(id_list[9]) * 3
        Y = S % 11
        M = 'F'
        JYM = '10X98765432'
        M = JYM[Y]
        if M == id_list[17] or M.lower()==id_list[17]:
            self.id_status = self.Errors[0]
        else:
            self.id_status = self.Errors[3]

    def _lookup_idcard(self):
        self._areaCheck()
        self._dateCheck()
        if self.id_status == self.Errors[0]:
            self.id_area = self.areas[self.idcard[:2]]
            self.id_birth = str(self._birthday())
            self.id_age = self._age()
            self.id_sex = self._sex()
        return {'idcard': self.idcard,
                'status': self.id_status,
                'area': self.id_area,
                'birthday': self.id_birth,
                'age': self.id_age,
                'sex': self.id_sex}

    @property
    def STATUS(self):
        return self._lookup_idcard()['status']

    @property
    def AREA(self):
        return self._lookup_idcard()['area']

    @property
    def BIRTHDAY(self):
        return self._lookup_idcard()['birthday']

    @property
    def AGE(self):
        return self._lookup_idcard()['age']

    @property
    def SEX(self):
        return self._lookup_idcard()['sex']


def getStatus(data):
    return IDCARD(data).STATUS


def getArea(data):
    return IDCARD(data).AREA


def getBirth(data):
    return IDCARD(data).BIRTHDAY


def getAge(data):
    return IDCARD(data).AGE


def getSex(data):
    return IDCARD(data).SEX


def getJson(data):
    return IDCARD(data)._lookup_idcard()


def getInfo(data, mode=None):
    if mode == 'STATUS':
        res = getStatus(data)
    elif mode == 'AREA':
        res = getArea(data)
    elif mode == 'BIRTHDAY':
        res = getBirth(data)
    elif mode == 'AGE':
        res = getAge(data)
    elif mode == 'SEX':
        res = getSex(data)
    else:
        res = getJson(data)
    return res

def idcard(value):
    from pyspark.sql import SparkSession
    from pyspark.sql import functions
    from pyspark.sql import HiveContext
    spark = SparkSession.builder \
        .master('yarn') \
        .appName('idcard') \
        .enableHiveSupport() \
        .getOrCreate()
    sc = spark.sparkContext
    hc = HiveContext(sc)
    hc.registerFunction('IDCARD', getInfo)
    hc.registerFunction('IDCARD_STATUS', getStatus)
    hc.registerFunction('IDCARD_AREA', getArea)
    hc.registerFunction('IDCARD_BIRTHDAY', getBirth)
    hc.registerFunction('IDCARD_AGE', getAge)
    hc.registerFunction('IDCARD_SEX', getSex)
    hc.sql(value).show()


if __name__ == '__main__':
    import sys
    idcard(sys.argv[1])

