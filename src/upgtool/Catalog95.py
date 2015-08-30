#coding=utf-8 

from catalog import Catalog
from tuple import HeapTuple
from relfile import RelFileNode
from relation import Relation
'''
 postgresql 9.5的属性表字段
'''
class PgAttribute95:
    def __init__(self,attrelid, attname, atttypid, attstattarget, attlen, attnum, 
                 attndims, attcacheoff, atttypmod, attbyval, attstorage, attalign, 
                 attnotnull, atthasdef, attisdropped, attislocal, attinhcount, attcollation):
        self.attrelid = attrelid
        self.attname = attname
        self.atttypid = atttypid
        self.attstattarget = attstattarget
        self.attlen = attlen
        self.attnum = attnum
        self.attndims = attndims
        self.attcacheoff = attcacheoff
        self.atttypmod = atttypmod
        self.attbyval = attbyval
        self.attstorage = attstorage
        self.attalign = attalign
        self.attnotnull = attnotnull
        self.atthasdef = atthasdef
        self.attisdropped = attisdropped
        self.attislocal = attislocal
        self.attinhcount = attinhcount
        self.attcollation = attcollation
        
        self.cacheoff = None



class Catalog95(Catalog):
    GLOBALTABLESPACE_OID = 1664
    DEFAULTTABLESPACE_OID = 1663
    RELMAPPER_FILEMAGIC = 0x592717
    MAX_MAPPINGS = 62
    NAMEDATALEN = 64
    heaptuple_class = HeapTuple

    def __init__(self, datadir):
        self.max_mappings = Catalog95.MAX_MAPPINGS
        Catalog.__init__(self, datadir)
 
    
    def getdatabaserelation(self):
        reloid = 1262
        if not self.pg_database_attr:
            self.pg_database_attr = [
                PgAttribute95( 1262, "datname", 19, -1, Catalog95.NAMEDATALEN, 1, 0, -1, -1, False, 'p', 'c', True, False, False, True, 0, 0 ), \
                PgAttribute95( 1262, "datdba", 26, -1, 4, 2, 0, -1, -1, True, 'p', 'i', True, False, False, True, 0, 0 ), \
                PgAttribute95( 1262, "encoding", 23, -1, 4, 3, 0, -1, -1, True, 'p', 'i', True, False, False, True, 0, 0 ), \
                PgAttribute95( 1262, "datcollate", 19, -1, Catalog95.NAMEDATALEN, 4, 0, -1, -1, False, 'p', 'c', True, False, False, True, 0, 0 ), \
                PgAttribute95( 1262, "datctype", 19, -1, Catalog95.NAMEDATALEN, 5, 0, -1, -1, False, 'p', 'c', True, False, False, True, 0, 0 ), \
                PgAttribute95( 1262, "datistemplate", 16, -1, 1, 6, 0, -1, -1, True, 'p', 'c', True, False, False, True, 0, 0 ), \
                PgAttribute95( 1262, "datallowconn", 16, -1, 1, 7, 0, -1, -1, True, 'p', 'c', True, False, False, True, 0, 0 ), \
                PgAttribute95( 1262, "datconnlimit", 23, -1, 4, 8, 0, -1, -1, True, 'p', 'i', True, False, False, True, 0, 0 ), \
                PgAttribute95( 1262, "datlastsysoid", 26, -1, 4, 9, 0, -1, -1, True, 'p', 'i', True, False, False, True, 0, 0 ), \
                PgAttribute95( 1262, "datfrozenxid", 28, -1, 4, 10, 0, -1, -1, True, 'p', 'i', True, False, False, True, 0, 0 ), \
                PgAttribute95( 1262, "datminmxid", 28, -1, 4, 11, 0, -1, -1, True, 'p', 'i', True, False, False, True, 0, 0 ), \
                PgAttribute95( 1262, "dattablespace", 26, -1, 4, 12, 0, -1, -1, True, 'p', 'i', True, False, False, True, 0, 0 ), \
                PgAttribute95( 1262, "datacl", 1034, -1, -1, 13, 1, -1, -1, False, 'x', 'i', False, False, False, True, 0, 0 )
            ]
        relfilenode = RelFileNode()
        relfilenode.space_node = Catalog95.GLOBALTABLESPACE_OID
        relfilenode.db_node = 0
        relfilenode.rel_node = self.getsharerelmap()[reloid]

        databaserelation = Relation(reloid, 'pg_database', self.pg_database_attr, relfilenode)
        return databaserelation
    
    def getpgclassrelation(self, tablesapce, dboid):
        reloid = 1259
        if not self.pg_class_attr:
            self.pg_class_attr = [
                PgAttribute95( reloid, "relname", 19, -1, Catalog95.NAMEDATALEN, 1, 0, -1, -1, False, 'p', 'c', True, False, False, True, 0, 0 ), \
                PgAttribute95( reloid, "relnamespace", 26, -1, 4, 2, 0, -1, -1, True, 'p', 'i', True, False, False, True, 0, 0 ), \
                PgAttribute95( reloid, "reltype", 26, -1, 4, 3, 0, -1, -1, True, 'p', 'i', True, False, False, True, 0, 0 ), \
                PgAttribute95( reloid, "reloftype", 26, -1, 4, 4, 0, -1, -1, True, 'p', 'i', True, False, False, True, 0, 0 ), \
                PgAttribute95( reloid, "relowner", 26, -1, 4, 5, 0, -1, -1, True, 'p', 'i', True, False, False, True, 0, 0 ), \
                PgAttribute95( reloid, "relam", 26, -1, 4, 6, 0, -1, -1, True, 'p', 'i', True, False, False, True, 0, 0 ), \
                PgAttribute95( reloid, "relfilenode", 26, -1, 4, 7, 0, -1, -1, True, 'p', 'i', True, False, False, True, 0, 0 ), \
                PgAttribute95( reloid, "reltablespace", 26, -1, 4, 8, 0, -1, -1, True, 'p', 'i', True, False, False, True, 0, 0 ), \
                PgAttribute95( reloid, "relpages", 23, -1, 4, 9, 0, -1, -1, True, 'p', 'i', True, False, False, True, 0, 0 ), \
                PgAttribute95( reloid, "reltuples", 700, -1, 4, 10, 0, -1, -1, self.float4ByVal, 'p', 'i', True, False, False, True, 0, 0 ), \
                PgAttribute95( reloid, "relallvisible", 23, -1, 4, 11, 0, -1, -1, True, 'p', 'i', True, False, False, True, 0, 0 ), \
                PgAttribute95( reloid, "reltoastrelid", 26, -1, 4, 12, 0, -1, -1, True, 'p', 'i', True, False, False, True, 0, 0 ), \
                PgAttribute95( reloid, "relhasindex", 16, -1, 1, 13, 0, -1, -1, True, 'p', 'c', True, False, False, True, 0, 0 ), \
                PgAttribute95( reloid, "relisshared", 16, -1, 1, 14, 0, -1, -1, True, 'p', 'c', True, False, False, True, 0, 0 ), \
                PgAttribute95( reloid, "relpersistence", 18, -1, 1, 15, 0, -1, -1, True, 'p', 'c', True, False, False, True, 0, 0 ), \
                PgAttribute95( reloid, "relkind", 18, -1, 1, 16, 0, -1, -1, True, 'p', 'c', True, False, False, True, 0, 0 ), \
                PgAttribute95( reloid, "relnatts", 21, -1, 2, 17, 0, -1, -1, True, 'p', 's', True, False, False, True, 0, 0 ), \
                PgAttribute95( reloid, "relchecks", 21, -1, 2, 18, 0, -1, -1, True, 'p', 's', True, False, False, True, 0, 0 ), \
                PgAttribute95( reloid, "relhasoids", 16, -1, 1, 19, 0, -1, -1, True, 'p', 'c', True, False, False, True, 0, 0 ), \
                PgAttribute95( reloid, "relhaspkey", 16, -1, 1, 20, 0, -1, -1, True, 'p', 'c', True, False, False, True, 0, 0 ), \
                PgAttribute95( reloid, "relhasrules", 16, -1, 1, 21, 0, -1, -1, True, 'p', 'c', True, False, False, True, 0, 0 ), \
                PgAttribute95( reloid, "relhastriggers", 16, -1, 1, 22, 0, -1, -1, True, 'p', 'c', True, False, False, True, 0, 0 ), \
                PgAttribute95( reloid, "relhassubclass", 16, -1, 1, 23, 0, -1, -1, True, 'p', 'c', True, False, False, True, 0, 0 ), \
                PgAttribute95( reloid, "relrowsecurity", 16, -1, 1, 24, 0, -1, -1, True, 'p', 'c', True, False, False, True, 0, 0 ), \
                PgAttribute95( reloid, "relispopulated", 16, -1, 1, 25, 0, -1, -1, True, 'p', 'c', True, False, False, True, 0, 0 ), \
                PgAttribute95( reloid, "relreplident", 18, -1, 1, 26, 0, -1, -1, True, 'p', 'c', True, False, False, True, 0, 0 ), \
                PgAttribute95( reloid, "relfrozenxid", 28, -1, 4, 27, 0, -1, -1, True, 'p', 'i', True, False, False, True, 0, 0 ), \
                PgAttribute95( reloid, "relminmxid", 28, -1, 4, 28, 0, -1, -1, True, 'p', 'i', True, False, False, True, 0, 0 ), \
                PgAttribute95( reloid, "relacl", 1034, -1, -1, 29, 1, -1, -1, False, 'x', 'i', False, False, False, True, 0, 0 ), \
                PgAttribute95( reloid, "reloptions", 1009, -1, -1, 30, 1, -1, -1, False, 'x', 'i', False, False, False, True, 0, 100)
            ]
        
        relfilenode = RelFileNode()
        relfilenode.space_node = tablesapce
        relfilenode.db_node = dboid
        relfilenode.rel_node = self.getdatabaserelmap(tablesapce, dboid)[reloid]
        relation = Relation(reloid, 'pg_class', self.pg_class_attr, relfilenode, tablesapce)
        
        return relation

 