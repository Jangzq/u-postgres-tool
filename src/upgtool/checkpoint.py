#coding=utf-8 

import struct

from pgstruct import memoffset
import pgstruct 

class CheckPoint:
    def __init__(self, beginaddr, startpos):
        self.redo, = struct.unpack_from('Q', beginaddr, startpos + memoffset['CheckPoint.redo'])
        self.ThisTimeLineID, = struct.unpack_from('I', beginaddr, startpos + memoffset['CheckPoint.ThisTimeLineID'])
        self.PrevTimeLineID, = struct.unpack_from('I', beginaddr, startpos + memoffset['CheckPoint.PrevTimeLineID'])
        self.fullPageWrites, = struct.unpack_from(pgstruct.get_type_format('bool'), beginaddr, startpos + memoffset['CheckPoint.fullPageWrites'])
        self.nextXidEpoch, = struct.unpack_from('I', beginaddr, startpos + memoffset['CheckPoint.nextXidEpoch'])
        self.nextXid, = struct.unpack_from('I', beginaddr, startpos + memoffset['CheckPoint.nextXid'])
        self.nextOid, = struct.unpack_from('I', beginaddr, startpos + memoffset['CheckPoint.nextOid'])
        self.nextMulti, = struct.unpack_from('I', beginaddr, startpos + memoffset['CheckPoint.nextMulti'])
        self.nextMultiOffset, = struct.unpack_from('I', beginaddr, startpos + memoffset['CheckPoint.nextMultiOffset'])
        self.oldestXid, = struct.unpack_from('I', beginaddr, startpos + memoffset['CheckPoint.oldestXid'])
        self.oldestXidDB, = struct.unpack_from('I', beginaddr, startpos + memoffset['CheckPoint.oldestXidDB'])
        self.oldestMulti, = struct.unpack_from('I', beginaddr, startpos + memoffset['CheckPoint.oldestMulti'])
        self.oldestMultiDB, = struct.unpack_from('I', beginaddr, startpos + memoffset['CheckPoint.oldestMultiDB'])
        self.time, = struct.unpack_from('Q', beginaddr, startpos + memoffset['CheckPoint.time'])
        self.oldestCommitTs, = struct.unpack_from('I', beginaddr, startpos + memoffset['CheckPoint.oldestCommitTs'])
        self.newestCommitTs, = struct.unpack_from('I', beginaddr, startpos + memoffset['CheckPoint.newestCommitTs'])
        self.oldestActiveXid, = struct.unpack_from('I', beginaddr, startpos + memoffset['CheckPoint.oldestActiveXid'])
