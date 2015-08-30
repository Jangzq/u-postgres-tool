#coding=utf-8 
'''
    封装了Relation。
'''
class Relation:
    SYS_COLUMNS = ['offsetnum', 'flag', 'xmin', 'xmax',
                    'cid', 'ctid', 'infomask', 'infomask2', 'oid']
    def __init__(self, reloid, relname, attrdesc, relfilenode, tablespace = 0):
        self.attrdesc = attrdesc
        self.relname = relname
        self.reloid = reloid
        self.relfilenode = relfilenode
        self.tablespace = tablespace
    
    def __cmp__(self, other):
        if self.relfilenode.space_node == other.relfilenode.space_node \
            and self.relfilenode.db_node == other.relfilenode.db_node \
            and self.relfilenode.rel_node == other.relfilenode.rel_node:
            return 0
        return 1
    
    def hassyscol(self, attrname):
        return attrname in self.SYS_COLUMNS
    
    def hascol(self, attrname):
        for attrdef in self.attrdesc:
            if attrname == attrdef.attname:
                return True
        return False
    
    def getcolname(self):
        return [x.attname for x in self.attrdesc]

