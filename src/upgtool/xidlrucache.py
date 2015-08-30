#coding=utf-8 

from controlfile import ControlFile
from lru import LruCache
from upgexception import UPgException

'''
对于multiXact和SubTransaction以及clog，它们的文件格式和读取方式有共同之处，所以封装了此cache，供这几类文件缓存使用。    
'''            
class XidLruCache:
    PAGES_PER_SEGMENT = 32
    def __init__(self, datadir, pageclass, buffsz):
        self.datadir = datadir
        ctrlfile = ControlFile(datadir)
        self.blcksz = ctrlfile.blcksz
        self.buffer = LruCache(pageclass, self.blcksz, buffsz)
    
    def getfilename(self, segno):
        pass
        
    def readfromdisk(self, pageno):
        blocksz = self.blcksz
        segno = pageno / self.PAGES_PER_SEGMENT
        filename = self.getfilename(segno)
        try:
            with open(filename) as file:
                file.seek(blocksz * pageno)
                block = file.read(blocksz)
        except:
            raise UPgException('error in reading xid')
        if len(block) != blocksz:
            raise UPgException('error in reading xid')
        return block

    def getlrupage(self, pageno):
        lrupage = self.buffer.get_and_visit(pageno)
        if not lrupage:
            block = self.readfromdisk(pageno)
            lrupage = self.buffer.put(pageno, block)
        return lrupage    
