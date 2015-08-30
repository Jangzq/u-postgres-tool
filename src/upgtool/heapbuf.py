#coding=utf-8 

'''
    封装了Heap buffer，使用LRU cache管理缓存的buffer。
'''
import struct
import logging
import os

from lru import LruCache
from lru import LruPage
from pgstruct import memoffset
from pgstruct import typesize

from upgexception import UPgException
from controlfile import ControlFile

logger = logging.getLogger("xlog")

HEAP_BUFFER_COUNT = 8192

MAIN_FORKNUM = 0
FSM_FORKNUM = 1
VISIBILITYMAP_FORKNUM = 2
INIT_FORKNUM = 3

'''
    封装了Heap buffer Page，继承自LruPage，即在u-postgres-tool里使用LRU管理heap buffer。
'''
class HeapBufferPage(LruPage):
    def load_data(self, key, data):
        LruPage.load_data(self, key,data)
        self.SizeOfPageHeaderData = memoffset['PageHeaderData.pd_linp']
        self.pd_lsn, = struct.unpack_from('Q', self.data, memoffset['PageHeaderData.pd_lsn'])
        self.pd_checksum, = struct.unpack_from('H', self.data, memoffset['PageHeaderData.pd_checksum'])
        self.pd_flags, = struct.unpack_from('H', self.data, memoffset['PageHeaderData.pd_flags'])
        self.pd_lower, = struct.unpack_from('H', self.data, memoffset['PageHeaderData.pd_lower'])
        self.pd_upper, = struct.unpack_from('H', self.data, memoffset['PageHeaderData.pd_upper'])
        self.pd_special, = struct.unpack_from('H', self.data, memoffset['PageHeaderData.pd_special'])
        self.pd_pagesize_version, = struct.unpack_from('H', self.data, memoffset['PageHeaderData.pd_pagesize_version'])
        
        
    def getmaxoffsetnumber(self):
        return 0 if self.pd_lower <= self.SizeOfPageHeaderData else (self.pd_lower- self.SizeOfPageHeaderData)/typesize['ItemIdData']
    
    def getitemid(self, offsetnum):
        value, = struct.unpack_from('I', self.data, memoffset['PageHeaderData.pd_linp'] + 4 * (offsetnum - 1))
        tupleoffset = value & 0x7fff
        flags = (value >> 15) & 0x3
        len = (value >> 17) & 0x7fff
        return tupleoffset, flags, len
'''
    封装了Heap buffer管理, 其中成员变量heapbuffer为LRU cache。
'''
class HeapBuffer:
    def __init__(self, pgdatadir, catalog_class):
        self.pgdatadir = pgdatadir
        self.catalog_class = catalog_class
        
        ctrlfile = ControlFile(pgdatadir)
        self.blocksz = ctrlfile.blcksz
        self.segfilesz = ctrlfile.relseg_size
        self.catalog_version_no = ctrlfile.catalog_version_no
        
        self.pg_majorversion = catalog_class.loadpgversion(pgdatadir)
        self.heapbuffer = LruCache(HeapBufferPage, self.blocksz, HEAP_BUFFER_COUNT)
    
    '''
        计算buffer tag。
    '''    
    def __getbuftag(self, relfilenode, forknum, blocknum):
        return struct.pack('5I', relfilenode.space_node, relfilenode.db_node, relfilenode.rel_node, forknum, blocknum)

    '''
        根据relfilenode得到文件名。根据global tablespace，default table space，还有指定了table space，文件名
        规则不一样。
    '''
    def getrelationpath(self, relfilenode, forknum):
        assert forknum == MAIN_FORKNUM or forknum == FSM_FORKNUM \
                        or forknum == VISIBILITYMAP_FORKNUM or forknum == INIT_FORKNUM
        pgdatadir = self.pgdatadir
        
        forkNames = ('', 'fsm', 'vm', 'init')
        if forknum == MAIN_FORKNUM:
            filename = relfilenode.rel_node
        else: 
            filename = '%u_%s'%(relfilenode.rel_node, forkNames[forknum])
        
        if relfilenode.space_node == self.catalog_class.GLOBALTABLESPACE_OID:
            return '%s/global/%s'%(pgdatadir, filename)
        elif relfilenode.space_node == self.catalog_class.DEFAULTTABLESPACE_OID:
            return '%s/base/%u/%s'%(pgdatadir, relfilenode.db_node, filename)
        else:
            tablespacedir =  "PG_%s_%u"%(self.pg_majorversion, self.catalog_version_no) 
            return '%s/pg_tblspc/%u/%s/%u/%s'%(pgdatadir, relfilenode.space_node, tablespacedir, relfilenode.db_node, filename)
        

    def __loadbuffer(self, relfilenode, forknum, blocknum):
        try:
            blocksz = self.blocksz
            segsz = self.segfilesz
        
            filepath = self.getrelationpath(relfilenode, forknum)
            segno = blocknum/segsz
            if segno > 0:
                filepath = '%s.%u'%(filepath, segno)
        
            blockoff = blocksz * (blocknum%segsz)
            with open(filepath) as file:
                file.seek(blockoff)
                block = file.read(blocksz)
                if len(block) != blocksz:
                    logger.error('could not read block %u in file \"%s\": %m'%(blocknum, filepath))
                    raise UPgException('could not read block in file')
            return block
        except IOError:
            logger.error('could not read block %u in file \"%s\": %m'%(blocknum, filepath))
            raise UPgException('could not read block in file')
            

    def readpage(self, relfilenode, forknum, blocknum):
        tag = self.__getbuftag(relfilenode, forknum, blocknum)
        buffpage = self.heapbuffer.get_and_visit(tag)
        if not buffpage:
            buffdata = self.__loadbuffer(relfilenode, forknum, blocknum)
            buffpage = self.heapbuffer.put(tag, buffdata)
        return buffpage

    def getblocknums(self, relfilenode, forknum):
        blocksz = self.blocksz
        segsz = self.segfilesz
            
        filepath = self.getrelationpath(relfilenode, forknum)
        segno = 0
        while True:
            if segno > 0:
                filepath = '%s.%u' % (filepath, segno)
            if not os.path.exists(filepath):
                return segno * segsz
            filesize = os.path.getsize(filepath)/blocksz
            if filesize > segsz:
                raise UPgException('could not read block in file')
            elif filesize < segsz:
                return segno * segsz + filesize
            else:
                segno += 1
