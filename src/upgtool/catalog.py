#coding=utf-8 

import logging

from controlfile import ControlFile
from heapbuf import HeapBuffer
from pgstruct import memoffset
from upgexception import UPgException
import pgstruct
import upgcrc


logger = logging.getLogger("Catalog")

'''
    不同的Pg版本有不同的文件布局，系统属性等等，这个类作为每个版本相应类的父类，记录相应信息。
'''
class Catalog:
    RELMAPPER_FILENAME = "pg_filenode.map"
    
    def __init__(self, datadir):
        self.datadir = datadir

        self.pg_majorversion = Catalog.loadpgversion(datadir)
        
        ctrlfile = ControlFile(datadir)
        self.catalog_version_no = ctrlfile.catalog_version_no
        self.float4ByVal = ctrlfile.float4ByVal
        self.float8ByVal = ctrlfile.float8ByVal
        self.nameDataLen = ctrlfile.nameDataLen
        
        self.pg_database_attr = None
        self.pg_class_attr = None
        self.globalrelmap = None
        self.databaserelmap = {}
        
    
    @staticmethod    
    def loadpgversion(datadir):
        filename = '%s/PG_VERSION'%(datadir)
        with open(filename, 'r') as file:
            return file.read().strip()

    @staticmethod 
    def getdbpath(pgdatadir, spacenode, dbnode, globaltablespace_oid, defaulttablespace_oid,
                  pg_majorversion, catalog_version_no):
        assert spacenode != globaltablespace_oid
        if spacenode == defaulttablespace_oid:
            return '%s/base/%u'%(pgdatadir, dbnode)
        else:
            tablespacedir =  "PG_%s_%u"%(pg_majorversion, catalog_version_no) 
            return '%s/pg_tblspc/%u/%s/%u'%(pgdatadir, spacenode, tablespacedir, dbnode)

    '''
        对于一些系统表如pg_class等，file oid和file node id的对应关系在map file里。
    '''
    def loadrelmap(self, spacenode=None, dbnode=None):
        assert not dbnode and not spacenode or dbnode and spacenode
        if not dbnode and not spacenode:
            mapfile = '%s/global/%s'%(self.datadir, Catalog.RELMAPPER_FILENAME)
        else:
            dbpath = Catalog.getdbpath(self.datadir, spacenode, dbnode, self.GLOBALTABLESPACE_OID, 
                                       self.DEFAULTTABLESPACE_OID, self.pg_majorversion, self.catalog_version_no)
            mapfile = '%s/%s'%(dbpath, Catalog.RELMAPPER_FILENAME)
        
        try:
            with open(mapfile, 'r') as file:
                data = file.read(pgstruct.typesize['RelMapFile'])
        except IOError as e:
            logger.error('could not read relation mapping file %s'%(mapfile))
            raise UPgException('could not read relation mapping file')
        
        if len(data) != pgstruct.typesize['RelMapFile']:
            logger.error('could not read relation mapping file %s'%(mapfile))
            raise UPgException('could not read relation mapping file')
        
        magic = pgstruct.getuint(data, memoffset['RelMapFile.magic'])
        mappingnum = pgstruct.getuint(data, memoffset['RelMapFile.num_mappings'])
        crc = pgstruct.getuint(data, memoffset['RelMapFile.crc'])
        
        
        if magic != self.RELMAPPER_FILEMAGIC or mappingnum > self.max_mappings:
            logger.error('mapping file %s contains invalid data'%(mapfile))
            raise UPgException('mapping file contains invalid data')
        
        crcdatas = []
        crcdatas.append((data, memoffset['RelMapFile.crc']));
        if not upgcrc.crceq(crcdatas, crc):
            logger.error('mapping file %s contains invalid crc'%(mapfile))
            raise UPgException('mapping file contains invalid crc')
        
        result = {}
        entrysize = pgstruct.typesize['RelMapping']
        for i in range(mappingnum):
            mapoid = pgstruct.getuint(data, memoffset['RelMapFile.mappings'] + i * entrysize+memoffset['RelMapping.mapoid'])
            mapfilenode = pgstruct.getuint(data, memoffset['RelMapFile.mappings'] + i * entrysize+memoffset['RelMapping.mapfilenode'])
            result[mapoid] = mapfilenode
        return result
    
    def getsharerelmap(self):
        if not self.globalrelmap:
            self.globalrelmap = self.loadrelmap();
        return self.globalrelmap
    
    def getdatabaserelmap(self, tablespace, dboid):
        try:
            return self.databaserelmap[dboid]
        except KeyError:
            relmap = self.loadrelmap(tablespace, dboid)
            self.databaserelmap[dboid] = relmap
            return relmap
