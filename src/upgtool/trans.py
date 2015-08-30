#coding=utf-8 
'''
    1. 封装了Sub trans和clog缓存。
    2. 提供了transaction相关操作。
'''

from lru import LruPage
from xidlrucache import XidLruCache
import pgstruct
from pgstruct import typesize
from upgexception import UPgException
import multixact

TRANSACTION_STATUS_IN_PROGRESS =     0x00 
TRANSACTION_STATUS_COMMITTED =       0x01
TRANSACTION_STATUS_ABORTED =         0x02   
TRANSACTION_STATUS_SUB_COMMITTED =   0x03   
TRANSACTION_STATUS_NOTFOUND = 0x04

class SubTransPage(LruPage):
    def load_data(self, key, data):
        LruPage.load_data(self, key, data)

class SubTransCache(XidLruCache):
    def __init__(self, datadir, buffsz = 32):
        XidLruCache.__init__(self, datadir, SubTransPage, buffsz)
        self.SUBTRANS_XACTS_PER_PAGE = self.blcksz / typesize['TransactionId']
        
    def getfilename(self, segno):
        return '%s/pg_subtrans/%04X'%(self.datadir, segno)    
    
    def getparentxid(self, xid):
        if not Trans.xidisnormal(xid):
            return 0
        pageno = xid / self.SUBTRANS_XACTS_PER_PAGE
        entry = xid % self.SUBTRANS_XACTS_PER_PAGE
        try:
            page = self.getlrupage(pageno)
        except UPgException:
            return 0
        
        parentxid = pgstruct.gettransactionid(page.data, entry*typesize['TransactionId'])
        return parentxid
        


class ClogPage(LruPage):
    def load_data(self, key, data):
        LruPage.load_data(self, key,data)

class ClogCache(XidLruCache):
    PAGES_PER_SEGMENT = 32
    CLOG_BITS_PER_XACT = 2
    CLOG_XACTS_PER_BYTE = 4
    def __init__(self, datadir, buffsz = 32):
        XidLruCache.__init__(self, datadir, ClogPage, buffsz)
        self.CLOG_XACTS_PER_PAGE = self.blcksz * self.CLOG_XACTS_PER_BYTE
        self.CLOG_XACT_BITMASK = ((1 << self.CLOG_BITS_PER_XACT) - 1)

    def get_transid_status(self, xid):
        pageno = xid / self.CLOG_XACTS_PER_PAGE
        byteno = (xid % self.CLOG_XACTS_PER_PAGE) / self.CLOG_XACTS_PER_BYTE
        bshift = (xid % self.CLOG_XACTS_PER_BYTE) * self.CLOG_BITS_PER_XACT
        try:
            blockdata = self.getlrupage(pageno).data
        except UPgException:
            return TRANSACTION_STATUS_NOTFOUND
        bytevalue = pgstruct.getubyte(blockdata, byteno)
        status = (bytevalue >> bshift) & self.CLOG_XACT_BITMASK;
        return status
    
    def getfilename(self, segno):
        return '%s/pg_clog/%04X'%(self.datadir, segno)    


class Trans:
    BootstrapTransactionId = 1
    FrozenTransactionId = 2
    FirstNormalTransactionId = 3
    
    def __init__(self, datadir, catalog_class):
        self.datadir = datadir
        self.clog = ClogCache(datadir)
        self.subtrans = SubTransCache(datadir)
        self.multixact = None
        self.catalog_class = catalog_class
    
    @staticmethod
    def xidisnormal(xid):
        return  xid >= Trans.FirstNormalTransactionId
    
    def getxidstatus(self, xid):
        if xid == self.BootstrapTransactionId or xid == self.FrozenTransactionId:
            return TRANSACTION_STATUS_COMMITTED
        
        if not self.xidisnormal(xid):
            return TRANSACTION_STATUS_ABORTED
        
        xidstatus = self.clog.get_transid_status(xid)
        return xidstatus

    def didcommit(self, xid):
        xidstatus = self.getxidstatus(xid)
        
        if xidstatus == TRANSACTION_STATUS_COMMITTED:
            return True
        
        if xidstatus == TRANSACTION_STATUS_SUB_COMMITTED:
            parentxid = self.subtrans.getparentxid(xid)
            if parentxid == 0:
                return False
            return self.didcommit(parentxid)
            
        return False    
    
    def getmultixact(self):
        if not multixact:
            multixact = multixact.Multixact(self.datadir, self.catalog_class);
        return multixact


def xmininvalid(infomask, catalog_class):
    return infomask & (catalog_class.heaptuple_class.HEAP_XMIN_COMMITTED|catalog_class.heaptuple_class.HEAP_XMIN_INVALID) \
        == catalog_class.heaptuple_class.HEAP_XMIN_INVALID

def xmincommited(infomask, catalog_class):
    return (infomask & catalog_class.heaptuple_class.HEAP_XMIN_COMMITTED) != 0

def xmaxislockedonly(infomask, catalog_class):
    return ((infomask & catalog_class.heaptuple_class.HEAP_XMAX_LOCK_ONLY) or \
     ((infomask & (catalog_class.heaptuple_class.HEAP_XMAX_IS_MULTI | catalog_class.heaptuple_class.HEAP_LOCK_MASK)) == catalog_class.heaptuple_class.HEAP_XMAX_EXCL_LOCK))

def xmaxcommited(infomask, catalog_class):
    return (infomask & catalog_class.heaptuple_class.HEAP_XMAX_COMMITTED) != 0
    

def satisfieslast(xmin, xmax, infomask, infomask2, trans):
    catalog_class = trans.catalog_class
    
    if xmininvalid(infomask, catalog_class):
        return False
    if not xmincommited(infomask, catalog_class) and not trans.didcommit(xmin):
        return False
    
    if xmax & catalog_class.heaptuple_class.HEAP_XMAX_INVALID:
        return True
    elif xmaxislockedonly(infomask, catalog_class):
        return True
    elif infomask & catalog_class.heaptuple_class.HEAP_XMAX_IS_MULTI:
        xmax = trans.getmultixact().getupdatexid(xmax, infomask, True)
    
    if xmaxcommited(infomask, catalog_class) or trans.didcommit(xmax):
        return False
    return True
    
def satisfies_include_uncommit(xmin, xmax, infomask, infomask2, trans):
    catalog_class = trans.catalog_class
    
    if xmax & catalog_class.heaptuple_class.HEAP_XMAX_INVALID:
        return True
    elif xmaxislockedonly(infomask, catalog_class):
        return True
    elif infomask & catalog_class.heaptuple_class.HEAP_XMAX_IS_MULTI:
        xmax = trans.getmultixact().getupdatexid(xmax, infomask, True)
    
    if xmaxcommited(infomask, catalog_class) or trans.didcommit(xmax):
        return False
    return True
    
