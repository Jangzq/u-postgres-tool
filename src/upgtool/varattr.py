#coding=utf-8 

'''
    封装了pg中var type相应操作。
'''
import struct

import pgstruct
from pgstruct import memoffset
from pgstruct import typesize

IS_BIGENDIAN = ord(struct.pack('I', 0x01020304)[0]) ==  1

VARTAG_INDIRECT = 1
VARTAG_ONDISK = 18


def getvartype(data, offset):
    b = pgstruct.getubyte(data, offset)
    if IS_BIGENDIAN:
        if b & 0x80 == 0:
            return '4B'
        if b & 0xC0 == 0:
            return '4B_U'
        if b & 0xC0 == 0x40:
            return '4B_C'
        if b & 0x80 == 0x80:
            return '1B'
        if b == 0x80:
            return '1B_E'
    else:
        if b & 0x01 == 0:
            return '4B'
        if b & 0x03 == 0:
            return '4B_U'
        if b & 0x03 == 0x02:
            return '4B_C'
        if b & 0x03 == 0x01:
            return '1B'
        if b  == 0x01:
            return '1B_E'
        

def getvarsize(data, offset):
    type = getvartype(data, offset)
    if type == '1B_E':
        return getexternalsize(data, offset)
    if type == '1B':
        return getvarsize1B(data, offset)
    return getvarsize4B(data, offset)

def getexternalsize(data, offset):
    tag = pgstruct.getubyte(data, offset + memoffset['varattrib_1b_e.va_tag'])
    return memoffset['varattrib_1b_e.va_data'] + typesize['varatt_indirect'] if tag == VARTAG_INDIRECT else typesize['varatt_external']

def getvarsize1B(data, offset):
    if IS_BIGENDIAN:
        return pgstruct.getubyte(data, offset + memoffset['varattrib_1b.va_header']) & 0x7F
    else:
        return (pgstruct.getubyte(data, offset + memoffset['varattrib_1b.va_header']) >> 1) & 0x7F

def getvarsize4B(data, offset):
    if IS_BIGENDIAN:
        return pgstruct.getubyte(data, offset + memoffset['varattrib_4b.va_4byte.va_header']) & 0x3FFFFFFF
    else:
        return (pgstruct.getubyte(data, offset + memoffset['varattrib_4b.va_4byte.va_header']) >> 2) & 0x3FFFFFFF
    
def strlen(data, offset, endset = 0):
    i = offset
    while i < (len(data) if not endset else min(len(data), endset))  and ord(data[i]) != 0:
        i += 1
    return i - offset
    
    
