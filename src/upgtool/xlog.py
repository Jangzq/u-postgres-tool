#coding=utf-8 
'''
封装xlog相关数据结构
'''
import logging
import struct
import re

from pgstruct import memoffset
import pgstruct

XLP_LONG_HEADER = 0x0002

logger = logging.getLogger("xlog")
class BackupLabel:
    def __init__(self, filename):
        self.filename = filename
        
    def load(self):
        try:
            findstart = False
            findcheckpoint = False
            findmethod = False
            findfrom = False
            with open(self.filename, 'r') as file:
                for line in file:
                    if not findstart:
                        m = re.match(r'START WAL LOCATION: ([0-9a-fA-F]+)/([0-9a-fA-F]+)',line)
                        if m:
                            self.startwal = (int(m.group(1), 16) << 32) | int(m.group(2), 16)
                            findstart = True
                    if not findcheckpoint:
                        m = re.match(r'CHECKPOINT LOCATION: ([0-9a-fA-F]+)/([0-9a-fA-F]+)',line)
                        if m:
                            self.checkpoint = (int(m.group(1), 16) << 32) | int(m.group(2), 16)
                            findcheckpoint = True
                    if not findmethod:
                        if line.startswith("BACKUP METHOD:"):
                            self.method = line[len("BACKUP METHOD:"):].strip()
                            findmethod = True
                    if not findfrom:
                        if line.startswith("BACKUP FROM:"):
                            self.backup_from = line[len("BACKUP FROM:"):].strip()
                            findfrom = True
        except IOError:
            logger.error("Error in reading control file!")
            raise
        

class XlogPageHeader:
    def __init__(self,  block_data):
        self.xlp_magic, = struct.unpack_from('H', block_data, memoffset['XLogPageHeaderData.xlp_magic'])
        self.xlp_info, = struct.unpack_from('H', block_data, memoffset['XLogPageHeaderData.xlp_info'])
        self.xlp_tli, = struct.unpack_from('I', block_data, memoffset['XLogPageHeaderData.xlp_tli'])
        self.xlp_pageaddr, = struct.unpack_from('Q', block_data, memoffset['XLogPageHeaderData.xlp_pageaddr'])
        self.xlp_rem_len, = struct.unpack_from('I', block_data, memoffset['XLogPageHeaderData.xlp_rem_len'])
        
        if self.xlp_info & XLP_LONG_HEADER:
            self.long_header = True
            self.xlp_sysid, = struct.unpack_from('Q', block_data, memoffset['XLogLongPageHeaderData.xlp_sysid'])
            self.xlp_seg_size, = struct.unpack_from('I', block_data, memoffset['XLogLongPageHeaderData.xlp_seg_size'])
            self.xlp_xlog_blcksz, = struct.unpack_from('I', block_data, memoffset['XLogLongPageHeaderData.xlp_xlog_blcksz'])
        else:
            self.long_header = False

class XlogRecord:
    def __init__(self, record_buf):
        self.xl_tot_len, = struct.unpack_from('I', record_buf, memoffset['XLogRecord.xl_tot_len'])  
        self.xl_xid, = struct.unpack_from('I', record_buf, memoffset['XLogRecord.xl_xid'])         
        self.xl_prev, = struct.unpack_from('Q', record_buf, memoffset['XLogRecord.xl_prev'])         
        self.xl_info, = struct.unpack_from('B', record_buf, memoffset['XLogRecord.xl_info'])         
        self.xl_rmid, = struct.unpack_from(pgstruct.get_type_format('RmgrId'), record_buf, memoffset['XLogRecord.xl_rmid'])         
        self.xl_crc, = struct.unpack_from('I', record_buf, memoffset['XLogRecord.xl_crc']) 
        self.blocks = []
        
