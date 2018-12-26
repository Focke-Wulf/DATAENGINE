# -*- coding: utf-8 -*-
# @Author: Eltar
# @Date: 2018-06-22 14:27:07
# @Last Modified by:   志翔ge
# @Last Modified time: 2018-08-06 17:49:03
from core.address import *

PCCJSON = 'pcc_2017.json'
ADDCUT = 'address_dict.txt'


class ADDRESS:

    __doc__ = """ Address information extraction """

    def __init__(self, address):
        """ define initial setting for address analysis """
        self.address = address
        # file path IO/management
        self.json_path = PCCJSON
        self.dict_path = ADDCUT
        self._load_pcc()
        # return function
        self.prov = ''
        self.city = ''
        self.area = ''
        self.loc = []

    def _load_pcc(self):
        with open(self.json_path, encoding='utf-8') as f:
            self.pccJson = json.load(f)

    def __nlp_cut__(self):
        nlp.load_userdict(self.dict_path)
        address_cut = [word for word in nlp.cut(self.address)]
        return address_cut

    def __address__(self, address_cut):

        # search for province name as key value
        word_list = address_cut.copy()
        prov = ''
        city = ''
        area = ''
        loc = []
        root = self.pccJson
        print(word_list)
        for word in word_list:
            
            if prov != '':
                break
            for p in root:
                if word in p:
                    prov = p
                    word_list.remove(word)
                    break

        root = root[prov]['child']
        for word in word_list:
            if city != '':
                break
            for c in root:
                if (word in c) or (word[:-1] in c and len(word[:-1]) >= 2) or (word[:-2] in c and len(word[:-2]) >= 2):
                    city = c
                    word_list.remove(word)
                    break

        for word in word_list:
            if area != '':
                break
            if city == '':
                for c in root:
                    if area != '':
                        break
                    for a in root[c]['child']:
                        if (word in a) or (word[:-1] in a and len(word[:-1]) >= 2) or (word[:-2] in a and len(word[:-2]) >= 2):
                            city = c
                            area = a
                            loc = root[c]['child'][a]['loc']
                            word_list.remove(word)
                            break
            else:
                for a in root[city]['child']:
                    if (word in a) or (word[:-1] in a and len(word[:-1]) >= 2) or (word[:-2] in a and len(word[:-2]) >= 2):
                        area = a
                        loc = root[city]['child'][a]['loc']
                        word_list.remove(word)
                        break

        self.prov = prov
        self.city = city
        self.area = area
        self.loc = loc

    def _lookup_address(self):
        address_cut = self.__nlp_cut__()
        self.__address__(address_cut)
        return {'address': self.address,
                'province': self.prov,
                'city': self.city,
                'area': self.area,
                'coordinate': self.loc}

    @property
    def PROVINCE(self):
        return self._lookup_address()['province']

    @property
    def CITY(self):
        return self._lookup_address()['city']

    @property
    def AREA(self):
        return self._lookup_address()['area']

    @property
    def COORDINATE(self):
        return self._lookup_address()['coordinate']


def getProv(data):
    return ADDRESS(data).PROVINCE


def getCity(data):
    return ADDRESS(data).CITY


def getArea(data):
    return ADDRESS(data).AREA


def getCoor(data):
    return ADDRESS(data).COORDINATE


def getJson(data):
    return ADDRESS(data)._lookup_address()


def getInfo(data, mode=None):
    if mode == 'PROVINCE':
        res = getProv(data)
    elif mode == 'CITY':
        res = getCity(data)
    elif mode == 'AREA':
        res = getArea(data)
    elif mode == 'COORDINATE':
        res = getCoor(data)
    else:
        res = getJson(data)
    return res


