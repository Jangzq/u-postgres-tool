#coding=utf-8 
'''
    封装了Multixact的操作。
    Multixact作为对外提供服务的类。
    MultixactOffsetCache是一个LRU cache，封装了pg_multixact/offsets缓存
    MultixactMemberCache也是一个LRU cache，封装了pg_multixact/members缓存
'''
import logging
import ctypes

from lru import LruPage
from xidlrucache import XidLruCache
import pgstruct
from pgstruct import typesize
from upgexception import UPgException

logger = logging.getLogger("multixact")

class MultiXactState:
    def __init__(self):
        self.oldestmultixactid = None
        self.nextmultixactid = None
        self.nextoffset = None
        

class MultixactOffPage(LruPage):
    pass

class MultixactMemPage(LruPage):
    pass


def uint32advancedIgnor0(xid, step):
    newvalue = xid+step;
    if newvalue > 0xFFFFFFFF:
        newvalue = newvalue - 0xFFFFFFFF
    return newvalue

def multixactidprecedes(multixid1, multixid2):
    return (ctypes.c_int32(multixid1).value - ctypes.c_int32(multixid2).value) < 0

'''
封装了pg_multixact/offsets缓存
'''
class MultixactOffsetCache(XidLruCache):
    FirstMultiXactId = 1
   
    def __init__(self, datadir, buffsz = 32):
        XidLruCache.__init__(self, datadir, MultixactOffPage, buffsz)
        self.MULTIXACT_OFFSETS_PER_PAGE = self.blcksz / typesize['MultiXactOffset']
   
    def getoffset(self, multixid, multixactstate = None):
        if multixactstate:
            if multixactidprecedes(multixid, multixactstate.oldestmultixactid):
                logger.error('MultiXactId %u does no longer exist -- apparent wraparound'%(multixid))
                return None
            if not multixactidprecedes(multixid, multixactstate.nextmultixactid):
                logger.error('MultiXactId %u has not been created yet -- apparent wraparound'%(multixid))
                return None
            
        pageno = multixid / self.MULTIXACT_OFFSETS_PER_PAGE
        entryno = multixid % self.MULTIXACT_OFFSETS_PER_PAGE
        try:
            page = self.getlrupage(pageno)
        except UPgException:
            return None
        offset = pgstruct.getmultixactoffset(page.data, entryno*typesize['MultiXactOffset'])
        return offset

    def getfilename(self, segno):
        return '%s/pg_multixact/offsets/%04X'%(self.datadir, segno)    
        

'''
封装了pg_multixact/members缓存
'''
class MultixactMemberCache(XidLruCache):
    MXACT_MEMBER_BITS_PER_XACT = 8
    MXACT_MEMBER_FLAGS_PER_BYTE = 1
    MXACT_MEMBER_XACT_BITMASK  =   ((1 << MXACT_MEMBER_BITS_PER_XACT) - 1)
    
    MULTIXACT_FLAGBYTES_PER_GROUP = 4
    MULTIXACT_MEMBERS_PER_MEMBERGROUP = MULTIXACT_FLAGBYTES_PER_GROUP * MXACT_MEMBER_FLAGS_PER_BYTE

    
    def __init__(self, datadir, offsetcache, buffsz = 32):
        XidLruCache.__init__(self, datadir, MultixactMemPage, buffsz)
        self.offsetcache = offsetcache

        self.MULTIXACT_MEMBERGROUP_SIZE = typesize['TransactionId'] * self.MULTIXACT_MEMBERS_PER_MEMBERGROUP + self.MULTIXACT_FLAGBYTES_PER_GROUP
        self.MULTIXACT_MEMBERGROUPS_PER_PAGE = (self.blcksz / self.MULTIXACT_MEMBERGROUP_SIZE)
        self.MULTIXACT_MEMBERS_PER_PAGE  = (self.MULTIXACT_MEMBERGROUPS_PER_PAGE * self.MULTIXACT_MEMBERS_PER_MEMBERGROUP)
    
    
    def __getmemberpagebyoffset(self, offset):
        return offset / self.MULTIXACT_MEMBERS_PER_PAGE
    
    def __getgroupflagoffset(self, offset):
        return ((offset / self.MULTIXACT_MEMBERS_PER_MEMBERGROUP) % \
                 self.MULTIXACT_MEMBERGROUPS_PER_PAGE) * \
                self.MULTIXACT_MEMBERGROUP_SIZE
    def __getmemberflagbitsshit(self, offset):
        return (offset % self.MULTIXACT_MEMBERS_PER_MEMBERGROUP) * \
            self.MXACT_MEMBER_BITS_PER_XACT

    def __getmemberinnerpageoffsetbyoffset(self,offset):
        return self.__getgroupflagoffset(offset) + self.MULTIXACT_FLAGBYTES_PER_GROUP + \
            (offset % self.MULTIXACT_MEMBERS_PER_MEMBERGROUP) * typesize['TransactionId']

 
    def getmembers(self, multixid, readtoinvalid = False, multixactstate = None):
        result = []
        thisoffset = self.offsetcache.getoffset(multixid, multixactstate)
        if not thisoffset:
            return None
        nextxid = uint32advancedIgnor0(multixid, 1)
        targetoffset = None
        if multixactstate and multixactstate.nextmultixactid == nextxid:
            targetoffset = multixactstate.nextoffset
        else:
            nextoffset = self.offsetcache.getoffset(nextxid, multixactstate)
            if not nextoffset and not readtoinvalid:
                logger.error("could not find next multi transaction's offset:%u"%(nextxid))
                return None
            if nextoffset:
                targetoffset = nextoffset
        
        offset = thisoffset    
        while True:
            if targetoffset and offset == targetoffset:
                break
            pageno = self.__getmemberpagebyoffset(offset)
            memberoff = self.__getmemberinnerpageoffsetbyoffset(offset)
            
            try:
                blockdata = self.getlrupage(pageno).data
            except UPgException:
                if targetoffset:
                    logger.error("could not read member block:%u"%(pageno))
                    return None
                else:
                    return result
            memberxid = pgstruct.gettransactionid(blockdata, memberoff)
            if memberxid == 0:
                if targetoffset:
                    logger.error("found invalid member transaction id in offset:%u"%(offset))
                    return None
                else:
                    return result
            flagoff = self.__getgroupflagoffset(offset)
            bshift = self.__getmemberflagbitsshit(offset)
            flag = pgstruct.getuint(blockdata, flagoff)
            status = (flag >> bshift) & self.MXACT_MEMBER_XACT_BITMASK;
            result.append((memberxid, status))
            offset = uint32advancedIgnor0(offset, 1)
        return result
    
    def getfilename(self, segno):
        return '%s/pg_multixact/members/%04X'%(self.datadir, segno)    
            
'''
    封装了multixact操作。
'''    
class Multixact:
    MultiXactStatusForKeyShare = 0x00
    MultiXactStatusForShare = 0x01
    MultiXactStatusForNoKeyUpdate = 0x02
    MultiXactStatusForUpdate = 0x03
    MultiXactStatusNoKeyUpdate = 0x04
    MultiXactStatusUpdate = 0x05

    def __init__(self, datadir, catalog_class):
        self.offsetcache = MultixactOffsetCache(datadir)
        self.membercache = MultixactMemberCache(datadir, self.offsetcache)
        self.tupleclass = catalog_class.heaptuple_class
        self.multixactstate = None
    
    def getmembers(self, multixid, readtoinvalid=None):
        return self.membercache.getmembers(multixid, readtoinvalid, self.multixactstate)   
    
    def getupdatexid(self, xmax, infomask, readtoinvalid=None):
        if not (infomask & self.tupleclass.HEAP_XMAX_IS_MULTI):
            logger.error("could not get update id if not HEAP_XMAX_IS_MULTI")
            raise UPgException("could not get update id if not HEAP_XMAX_IS_MULTI")
        
        members = self.getmembers(xmax, readtoinvalid)
        if not members:
            logger.error("could not get multixact members for %u"%(xmax))
            raise UPgException('could not get multixact members')
        
        for memxid, status in members:
            if status >  self.MultiXactStatusForUpdate:
                return memxid
            
        
                 
    
