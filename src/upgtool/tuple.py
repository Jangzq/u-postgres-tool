#coding=utf-8 
'''
    封装了Heap tuple。
'''
import ctypes

from pgstruct import memoffset
from pgstruct import att_align_nominal
from pgstruct import att_align_pointer
import pgstruct
import varattr
import attrtype

def getmaskstr(maskvalue, maskdesc):
    result = []
    for (desc, mask) in maskdesc:
        if maskvalue & mask != 0:
            result.append(desc)
    return result

class HeapTupleHeader:
    def load(self, pagedata, tupleoff, heaptuplemode = True):
        if heaptuplemode:
            self.xmin = pgstruct.getuint(pagedata, 
                tupleoff+memoffset['HeapTupleHeaderData.t_heap']+memoffset['HeapTupleFields.t_xmin'])
            self.xmax = pgstruct.getuint(pagedata, 
                tupleoff+memoffset['HeapTupleHeaderData.t_heap']+memoffset['HeapTupleFields.t_xmax'])
            self.field3 = pgstruct.getuint(pagedata, 
                tupleoff+memoffset['HeapTupleHeaderData.t_heap']+memoffset['HeapTupleFields.t_field3'])
        else:
            self.datum_len_ = pgstruct.getuint(pagedata, 
                tupleoff+memoffset['HeapTupleHeaderData.t_heap']+memoffset['DatumTupleFields.datum_len_'])
            self.datum_typmod = pgstruct.getuint(pagedata, 
                tupleoff+memoffset['HeapTupleHeaderData.t_heap']+memoffset['DatumTupleFields.datum_typmod'])
            self.datum_typeid = pgstruct.getuint(pagedata, 
                tupleoff+memoffset['HeapTupleHeaderData.t_heap']+memoffset['DatumTupleFields.datum_typeid'])
        
        self.ctid_blkid_hi =  pgstruct.getushort(pagedata, 
                tupleoff+memoffset['HeapTupleHeaderData.t_ctid']
                +memoffset['ItemPointerData.ip_blkid'] + memoffset['BlockIdData.bi_hi'])
        self.ctid_blkid_lo =  pgstruct.getushort(pagedata, 
                tupleoff+memoffset['HeapTupleHeaderData.t_ctid']
                +memoffset['ItemPointerData.ip_blkid'] + memoffset['BlockIdData.bi_lo'])
        self.ctid_posid =  pgstruct.getushort(pagedata, 
                tupleoff+memoffset['HeapTupleHeaderData.t_ctid']
                +memoffset['ItemPointerData.ip_posid'])
        
        self.infomask2 = pgstruct.getushort(pagedata, 
                tupleoff+memoffset['HeapTupleHeaderData.t_infomask2'])
        self.infomask = pgstruct.getushort(pagedata, 
                tupleoff+memoffset['HeapTupleHeaderData.t_infomask'])
        
        self.hoff = pgstruct.getubyte(pagedata, 
                tupleoff+memoffset['HeapTupleHeaderData.t_hoff'])
        
        hasoid = (self.infomask & HeapTuple.HEAP_HASOID) != 0
        hasnull = (self.infomask & HeapTuple.HEAP_HASNULL) != 0
        if hasnull:
            nullbitlen = self.hoff - memoffset['HeapTupleHeaderData.t_bits']- (4 if hasoid else 0)
            self.bits = ctypes.string_at(ctypes.byref(pagedata, tupleoff+memoffset['HeapTupleHeaderData.t_bits']), nullbitlen)
        else:
            nullbitlen = 0
            self.bits = None
        if hasoid:
            self.oid = pgstruct.getuint(pagedata, tupleoff+ self.hoff - 4)
        else:
            self.oid = None
    
    def attisnull(self, attrnum):
        return not (pgstruct.getubyte(self.bits, attrnum >> 3) & (1 << ((attrnum) & 0x07)))    

class HeapTuple:
    HEAP_HASNULL =           0x0001 
    HEAP_HASVARWIDTH =       0x0002 
    HEAP_HASEXTERNAL =       0x0004 
    HEAP_HASOID =            0x0008 
    HEAP_XMAX_KEYSHR_LOCK =  0x0010 
    HEAP_COMBOCID =          0x0020 
    HEAP_XMAX_EXCL_LOCK =    0x0040 
    HEAP_XMAX_LOCK_ONLY =    0x0080 
    HEAP_XMIN_COMMITTED =    0x0100 
    HEAP_XMIN_INVALID =      0x0200 
    HEAP_XMIN_FROZEN =       (HEAP_XMIN_COMMITTED|HEAP_XMIN_INVALID)
    HEAP_XMAX_COMMITTED =    0x0400 
    HEAP_XMAX_INVALID =      0x0800 
    HEAP_XMAX_IS_MULTI =     0x1000 
    HEAP_UPDATED =           0x2000 
    HEAP_MOVED_OFF =         0x4000 
    HEAP_MOVED_IN =          0x8000     
    
    HEAP_XMAX_SHR_LOCK =  (HEAP_XMAX_EXCL_LOCK | HEAP_XMAX_KEYSHR_LOCK)
    HEAP_LOCK_MASK =  (HEAP_XMAX_SHR_LOCK | HEAP_XMAX_EXCL_LOCK | \
                         HEAP_XMAX_KEYSHR_LOCK)



    HEAP_NATTS_MASK =        0x07FF 
    HEAP_KEYS_UPDATED =      0x2000 
    HEAP_HOT_UPDATED =       0x4000 
    HEAP_ONLY_TUPLE =        0x8000 
    HEAP2_XACT_MASK =        0xE000 

    INFOMASK_DESC = (('HEAP_HASNULL',           HEAP_HASNULL), 
        ('HEAP_HASVARWIDTH',       HEAP_HASVARWIDTH), 
        ('HEAP_HASEXTERNAL',       HEAP_HASEXTERNAL), 
        ('HEAP_HASOID',               HEAP_HASOID),
        ('HEAP_XMAX_KEYSHR_LOCK',   HEAP_XMAX_KEYSHR_LOCK),
        ('HEAP_COMBOCID',           HEAP_COMBOCID),
        ('HEAP_XMAX_EXCL_LOCK',       HEAP_XMAX_EXCL_LOCK),
        ('HEAP_XMAX_LOCK_ONLY',       HEAP_XMAX_LOCK_ONLY),
        ('HEAP_XMIN_COMMITTED',       HEAP_XMIN_COMMITTED),
        ('HEAP_XMIN_INVALID',       HEAP_XMIN_INVALID),
        ('HEAP_XMAX_COMMITTED',       HEAP_XMAX_COMMITTED),
        ('HEAP_XMAX_INVALID',       HEAP_XMAX_INVALID),
        ('HEAP_XMAX_IS_MULTI',       HEAP_XMAX_IS_MULTI),
        ('HEAP_UPDATED',           HEAP_UPDATED), 
        ('HEAP_MOVED_OFF',           HEAP_MOVED_OFF),
        ('HEAP_MOVED_IN',           HEAP_MOVED_IN))
    
    INFOMASK2_DESC = (
        ('HEAP_KEYS_UPDATED',       0x2000), 
        ('HEAP_HOT_UPDATED',       0x4000),  
        ('HEAP_ONLY_TUPLE',           0x8000)
    )
    
    
    @staticmethod
    def getinfomaskdesc(maskvalue):
        match = getmaskstr(maskvalue, HeapTuple.INFOMASK_DESC)
        if 'HEAP_XMAX_EXCL_LOCK' in match and 'HEAP_XMAX_KEYSHR_LOCK' in match:
            match.remove('HEAP_XMAX_KEYSHR_LOCK')
            match.remove('HEAP_XMAX_EXCL_LOCK')
            match.append('HEAP_XMAX_SHR_LOCK')
        
        if 'HEAP_XMIN_COMMITTED' in match and 'HEAP_XMIN_INVALID' in match:
            match.remove('HEAP_XMIN_COMMITTED')
            match.remove('HEAP_XMIN_INVALID')
            match.append('HEAP_XMIN_FROZEN')
        return ','.join(match)
    
    @staticmethod
    def getinfomask2desc(maskvalue):
        match = getmaskstr(maskvalue, HeapTuple.INFOMASK2_DESC)
        
        HEAP_NATTS_MASK = 0x07FF
        nattrs = maskvalue & HEAP_NATTS_MASK
        match.append('%u attrs'%(nattrs))
        return ','.join(match)
         

    def __init__(self, relation, pagedata, offnum, tupleoff, tupleflag, tuplelen):
        self.relation = relation
        self.pagedata = pagedata
        self.offnum = offnum
        self.tupleoff = tupleoff
        self.tupleflag = tupleflag
        self.tuplelen = tuplelen
        
        self.flagdesc = ['LP_UNUSED', 'LP_NORMAL', 'LP_REDIRECT', 'LP_DEAD']
        self.heapheader = None
        
    def getflagstr(self):
        return self.flagdesc[self.tupleflag]
    
    
    def gettupleheader(self):
        if not self.heapheader:
            self.heapheader = HeapTupleHeader()
            self.heapheader.load(self.pagedata, self.tupleoff)
        return self.heapheader
    
    def __fetchattr(self, attrdef, dataoff, attrsize):
        typid = attrdef.atttypid
        atttyp = attrtype.getatttype(typid)
        if not atttyp:
            return 'Not support type.'
        return atttyp.typoutput(self.pagedata, dataoff, attrsize) 
        
        
    
    
    def __getaddlength(self, attrdef, offset):
        if attrdef.attlen > 0:
            return attrdef.attlen
        if attrdef.attlen == -1:
            return varattr.getvarsize(self.pagedata, offset)
        if attrdef.attlen == -2:
            return varattr.strlen(self.pagedata, offset)
        
    """heaptuple.c::heap_deform_tuple"""
    def deformtuple(self):
        result = {}
        tupleheader = self.gettupleheader()
        
        hasnull = tupleheader.infomask & HeapTuple.HEAP_HASNULL != 0
        nattrs = tupleheader.infomask2 & HeapTuple.HEAP_NATTS_MASK
        
        attrdefs = self.relation.attrdesc
        nattrs = min(nattrs, len(attrdefs))
        slow = False
        off = 0
        tupledataoff = self.tupleoff + tupleheader.hoff 
        
        for attrnum in range(nattrs):
            attrdef = attrdefs[attrnum]
            if hasnull and tupleheader.attisnull(attrnum):
                result[attrdef.attname] = None
                slow = True
                continue
            if not slow and attrdef.cacheoff :
                off = attrdef.cacheoff
            elif attrdef.attlen == -1:
                if not slow and off == att_align_nominal(off, attrdef.attalign):
                    attrdef.cacheoff = off
                else:
                    off = att_align_pointer(off, attrdef.attalign, -1, self.pagedata,  tupledataoff)
                    slow = True
            else:
                off = att_align_nominal(off, attrdef.attalign)
                if not slow:
                    attrdef.cacheoff = off
                    
            attrsize = self.__getaddlength(attrdef, tupledataoff+off) 
            result[attrdef.attname] = self.__fetchattr(attrdef, tupledataoff+off, attrsize)
            off += attrsize
            if attrdef.attlen <= 0:
                slow = True
                
        return result
    
            
    
    