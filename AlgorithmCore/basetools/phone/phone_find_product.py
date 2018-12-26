# -*- coding: utf-8 -*-
# @Author: 谷志翔
# @Date:   2018-07-06 16:30:51
# @Last Modified by:   谷志翔
# @Last Modified time: 2018-08-01 16:40:04

import os
import struct
import sys


class PHONE(object):
    __doc__ = '''手机信息查询模块'''

    def __init__(self, phone_num, dat_file=None):

        if dat_file is None:
            dat_file = "phone.dat"

        with open(dat_file, 'rb') as f:
            self.buf = f.read()

        self.head_fmt = "<4si"
        self.phone_fmt = "<iiB"
        self.head_fmt_length = struct.calcsize(self.head_fmt)
        self.phone_fmt_length = struct.calcsize(self.phone_fmt)
        self.version, self.first_phone_record_offset = struct.unpack(
            self.head_fmt, self.buf[:self.head_fmt_length])
        self.phone_record_count = (len(
            self.buf) - self.first_phone_record_offset) / self.phone_fmt_length
        self.phone_num = phone_num

    def get_phone_dat_msg(self):
        print("版本号:{}".format(self.version))
        print("总记录条数:{}".format(self.phone_record_count))

    @staticmethod
    def get_phone_no_type(no):
        if no == 4:
            return "电信虚拟运营商"
        if no == 5:
            return "联通虚拟运营商"
        if no == 6:
            return "移动虚拟运营商"
        if no == 3:
            return "电信"
        if no == 2:
            return "联通"
        if no == 1:
            return "移动"

    @staticmethod
    def _format_phone_content(phone_num, record_content, operator):

        province, city, zip_code, area_code = record_content.split('|')
        return {
            "phone": phone_num,
            "province": province,
            "city": city,
            "zip_code": zip_code,
            "area_code": area_code,
            "operator": PHONE.get_phone_no_type(operator)
        }

    def _lookup_phone(self):

        phone_num = str(self.phone_num)
        assert 7 <= len(self.phone_num) <= 11
        int_phone = int(str(self.phone_num)[0:7])

        left = 0
        right = self.phone_record_count
        while left <= right:
            middle = int((left + right) / 2)
            current_offset = int(
                self.first_phone_record_offset + middle * self.phone_fmt_length)
            if current_offset >= len(self.buf):
                return

            buffer = self.buf[current_offset: current_offset +
                              self.phone_fmt_length]
            cur_phone, record_offset, operator = struct.unpack(self.phone_fmt,
                                                               buffer)

            if cur_phone > int_phone:
                right = middle - 1
            elif cur_phone < int_phone:
                left = middle + 1
            else:
                record_content = PHONE.get_record_content(
                    self.buf, record_offset)
                return PHONE._format_phone_content(phone_num, record_content,
                                                   operator)

    def find(self):
        return self._lookup_phone()

    @property
    def PROVINCE(self):
        return self._lookup_phone()['province']

    @property
    def CITY(self):
        return self._lookup_phone()['city']

    @property
    def ZIPCODE(self):
        return self._lookup_phone()['zip_code']

    @property
    def AREACODE(self):
        return self._lookup_phone()['area_code']

    @property
    def OPERATOR(self):
        return self._lookup_phone()['operator']

    @staticmethod
    def human_phone_info(phone_info):
        if not phone_info:
            return ''

        return "{}|{}|{}|{}|{}|{}".format(phone_info['phone'],
                                          phone_info['province'],
                                          phone_info['city'],
                                          phone_info['zip_code'],
                                          phone_info['area_code'],
                                          phone_info['operator'])

    @staticmethod
    def get_record_content(buf, start_offset):
        end_offset = start_offset + buf[start_offset:-1].find(b'\x00')
        return buf[start_offset:end_offset].decode()


def getProv(data):
    return PHONE(data).PROVINCE


def getCity(data):
    return PHONE(data).CITY


def getZip(data):
    return PHONE(data).ZIPCODE


def getArea(data):
    return PHONE(data).AREACODE


def getOper(data):
    return PHONE(data).OPERATOR


def getJson(data):
    return PHONE(data)._lookup_phone()


def getInfo(data, mode=None):
    if mode == 'PROVINCE':
        res = getProv(data)
    elif mode == 'CITY':
        res = getCity(data)
    elif mode == 'ZIPCODE':
        res = getZip(data)
    elif mode == 'AREACODE':
        res = getArea(data)
    elif mode == 'OPERATOR':
        res = getOper(data)
    else:
        res = getJson(data)
    return res

def phone(value):
    from pyspark.sql import SparkSession
    from pyspark.sql import functions
    from pyspark.sql import HiveContext
    spark = SparkSession.builder \
        .master('yarn') \
        .appName('phone') \
        .getOrCreate()
    sc = spark.sparkContext
    sc.addFile('/root/DataEngine/python/core/phone/phone.dat')
    hc = HiveContext(sc)
    hc.registerFunction('PHONE', getInfo)
    hc.registerFunction('PHONE_PROVINCE', getProv)
    hc.registerFunction('PHONE_CITY', getCity)
    hc.registerFunction('PHONE_ZIPCODE', getZip)
    hc.registerFunction('PHONE_AREACODE', getArea)
    hc.registerFunction('PHONE_OPERATOR', getOper)
    hc.sql(value).show()


if __name__ == '__main__':
    import sys
    # phone(sys.argv[1])
    phone("select PHONE_OPERATOR(phone) from phone_test")


