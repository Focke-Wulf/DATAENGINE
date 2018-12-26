# -*- coding: utf-8 -*-
# @Author: 谷志翔
# @Date:   2018-08-07 12:43:48
# @Last Modified by:   谷志翔
# @Last Modified time: 2018-08-07 12:45:23


def transInfo(x, start, end=None, symbol='*'):
    string = str(x)
    if end == None or end == start:
        tmp = list(string)
        tmp[start - 1] = symbol
        string = ''.join(tmp)
    else:
        tmp = string[start - 1:end]
        string = string.replace(tmp, symbol * len(tmp))
    return string


def fillInfo(x, filler='missing'):
    return x if x != None else filler
