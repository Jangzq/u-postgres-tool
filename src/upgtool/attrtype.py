#coding=utf-8 
'''
    封装了pg的数据类型。
'''
import ctypes

import varattr
import pgstruct

atttypes = {}

def getatttype(typid):
    try:
        return atttypes[typid]
    except:
        return None


def boolin():
    pass

def boolout(data, off, len):
    if pgstruct.getubyte(data, off) != 0:
        return 'True'
    else:
        return 'False'

def charin():
    pass

def charout(data, off, len):
    pass

def int8in():
    pass

def int8out(data, off, len):
    pass

def int2in():
    pass

def int2out(data, off, len):
    pass

def int4in():
    pass

def int4out(data, off, len):
    return pgstruct.getint(data, off)

def textin():
    pass

def textout(data, off, len):
    pass

def float4in():
    pass

def float4out(data, off, len):
    pass

def float8in():
    pass

def float8out(data, off, len):
    pass

def varcharin():
    pass

def varcharout(data, off, len):
    pass

def date_in():
    pass

def date_out(data, off, len):
    pass

def time_in():
    pass

def time_out(data, off, len):
    pass

def timestamp_in():
    pass

def timestamp_out(data, off, len):
    pass

def timestamptz_in():
    pass

def timestamptz_out(data, off, len):
    pass

def numeric_in():
    pass

def numeric_out(data, off, len):
    pass

def namein():
    pass

def oidin():
    pass

def oidout(data, off, len):
    return pgstruct.getuint(data, off)

def xidin():
    pass

def xidout(data, off, len):
    return pgstruct.getuint(data, off)


def nameout(data, off, len):
    varlen = varattr.strlen(data, off, off+len)
    return ctypes.string_at(ctypes.byref(data, off), varlen)

def inittypes():
    PgType(16, 'bool', boolin, boolout, True, 1)
    PgType(18, 'char', charin, charout, True, 1)
    PgType(19, 'name', namein, nameout, False, 64) 
    PgType(20, 'int8', int8in, int8out, True, 8) 
    PgType(21, 'int2', int2in, int2out, True, 2)
    PgType(23, 'int4', int4in, int4out, True, 4)
    PgType(25, 'text', textin, textout, False, -1)
    PgType(26, 'oid', oidin, oidout, True, 4)
    PgType(28, 'xid', xidin, xidout, True, 4)
    PgType(700, 'float4', float4in, float4out, True, 4)
    PgType(701, 'float8', float8in, float8out, True, 8)
    PgType(1043, 'varchar', varcharin, varcharout, False, -1)
    PgType(1082, 'date', date_in, date_out, True, 4)
    PgType(1083, 'time', time_in, time_out, True, 8)
    PgType(1114, 'timestamp', timestamp_in, timestamp_out, True, 8)
    PgType(1184, 'timestamptz', timestamptz_in, timestamptz_out, True, 8)
    PgType(1700, 'numeric', numeric_in, numeric_out, False, -1)
   
    


class PgType:
    def __init__(self, oid, typename, typinput, typoutput, typbyval, typlen):
        self.oid = oid
        self.typename = typename
        self.typinput = typinput
        self.typoutput = typoutput
        self.typbyval = typbyval
        self.typlen = typlen
        atttypes[oid] = self
    
   
inittypes()    
    
    
