#coding=utf-8 
'''
    在u-postgres-tool里，并没有使用ctypes来完成c结构和python之间的映射，而是采用了一个配置文件，记录了每个成员变量的偏移，而这个文件是由一个c程序输出而来，这样做的目的是：
    1. 能比较简单的完成结构体的映射
    2. 支持不同pg版本时，只需使用不同文件即可。
    3. 如果使用的pg，是采用特殊的对齐方式，只需重新生成配置文件即可。
    4. 可以支持交叉运行。
'''
import struct
import logging
import string

from upgexception import UPgException

logger = logging.getLogger("pg-struct")
memoffset = {}
typesize = {}


def getuint(data, offset):
    return struct.unpack_from('I', data, offset)[0]

def getint(data, offset):
    return struct.unpack_from('i', data, offset)[0]

def getushort(data, offset):
    return struct.unpack_from('H', data, offset)[0]

def getubyte(data, offset):
    return struct.unpack_from('B', data, offset)[0]

def gettransactionid(data, offset):
    return getuint(data, offset)

def getmultixactoffset(data, offset):
    return getuint(data, offset)

def get_type_format(typename):
    size = typesize[typename]
    if size == 1:
        return 'B'
    if size == 2:
        return 'H'
    if size == 4:
        return 'I'
    if size == 8:
        return 'Q'
    raise UPgException("unsupported type size.")

def load_offset_cfg(filename):
    try:
        with open(filename, 'r') as file:
            for aline in file:
                thisline = aline.strip()
                if thisline == '' or thisline.startswith('#'):
                    continue
                tokens = thisline.split('=')
                if len(tokens) !=2:
                    logger.error('The offset file format is error!')
                    raise UPgException('The offset file format is error!')
                key = tokens[0]
                value = tokens[1]
                if not value.isdigit():
                    logger.error('The offset file format is error!')
                    raise UPgException('The offset file format is error!')
                memoffset[key] = string.atoi(value)
    except IOError:
        logger.error('IO error in processing offset file!')
        raise
                

def load_type_size(filename):
    try:
        with open(filename, 'r') as file:
            for aline in file:
                thisline = aline.strip()
                if thisline == '' or thisline.startswith('#'):
                    continue
                tokens = thisline.split('=')
                if len(tokens) !=2:
                    logger.error('The type size file format is error!')
                    raise UPgException('The type size file format is error!')
                key = tokens[0]
                value = tokens[1]
                if not value.isdigit():
                    logger.error('The type size file  is error!')
                    raise UPgException('The type size file  is error!')
                typesize[key] = string.atoi(value)
    except IOError:
        logger.error('IO error in processing offset file!')
        raise


def TYPEALIGN(alignval, len):
    return (len + alignval - 1) & ~(alignval - 1)
 
def TYPEALIGN_DOWN(alignval, len):
    return len & ~(alignval - 1)


def SHORTALIGN(len):
    return TYPEALIGN(typesize['ALIGNOF_SHORT'], len)

def INTALIGN(len):
    return TYPEALIGN(typesize['ALIGNOF_INT'], len)

def LONGALIGN(len):
    return TYPEALIGN(typesize['ALIGNOF_LONG'], len)

def DOUBLEALIGN(len):
    return TYPEALIGN(typesize['ALIGNOF_DOUBLE'], len)

def SHORTALIGN_DOWN(len):
    return TYPEALIGN_DOWN(typesize['ALIGNOF_SHORT'], len)

def INTALIGN_DOWN(len):
    return TYPEALIGN_DOWN(typesize['ALIGNOF_INT'], len)

def LONGALIGN_DOWN(len):
    return TYPEALIGN_DOWN(typesize['ALIGNOF_LONG'], len)

def DOUBLEALIGN_DOWN(len):
    return TYPEALIGN_DOWN(typesize['ALIGNOF_DOUBLE'], len)

def MAXALIGN_DOWN(len):
    return TYPEALIGN_DOWN(typesize['MAXIMUM_ALIGNOF'], len)

def att_align_nominal(curoff, attralign):
    if attralign == 'i':
        return INTALIGN(curoff)
    if attralign == 'c':
        return curoff
    if attralign == 'd':
        return DOUBLEALIGN(curoff)
    return SHORTALIGN(curoff)

def att_align_pointer(cur_offset, attalign, attlen, pagedata, tupledataoff):
    if attlen == -1 and pagedata[tupledataoff+cur_offset] != 0:
        return cur_offset
    return att_align_nominal(cur_offset, attalign)
