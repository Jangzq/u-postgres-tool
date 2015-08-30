#coding=utf-8 
'''
    封装relation file相关类及操作
'''
import struct

from pgstruct import memoffset

class RelFileNode:
    def load(self, buff):
        self.space_node = struct.unpack_from('I', buff, memoffset['RelFileNode.spcNode'])    
        self.db_node = struct.unpack_from('I', buff, memoffset['RelFileNode.dbNode'])
        self.rel_node = struct.unpack_from('I', buff, memoffset['RelFileNode.relNode'])
    def setvalue(self, space_node, db_node, rel_node):
        self.space_node = space_node
        self.db_node = db_node
        self.rel_node = rel_node
