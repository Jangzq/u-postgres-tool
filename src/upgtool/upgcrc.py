#coding=utf-8 

import logging
import ctypes
import os
from upgexception import UPgException

logger = logging.getLogger("pg-crc")
crctool = None

def initcrc(dllfilename):
    global crctool
    if os.path.exists(dllfilename):
        try:
            crctool = ctypes.CDLL(dllfilename)
        except:
            logger.error("error in loading crc dll")
            raise UPgException("error in loading crc dll")


def crceq(datas, expect_crc):
    if not crctool:
        return True
    crc = 0xFFFFFFFF
    for (data, datalen) in datas:
        crc = crctool.comp_crc32c(crc, data, datalen)&0xFFFFFFFF
    
    crc ^= 0xFFFFFFFF
    return crc == expect_crc
