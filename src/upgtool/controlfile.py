#coding=utf-8 

import struct
import logging

import pgstruct
import upgcrc
from pgstruct import memoffset
from upgexception import UPgException
from checkpoint import CheckPoint





PG_CONTROL_SIZE = 8192
logger = logging.getLogger("xlog")

class ControlFile:
    def __init__(self, datadir):
        self.filename = "%s/global/pg_control"%(datadir)
        self.__load(self.filename)
        
    def __load(self, filename):
        try:
            with open(filename, 'rb') as file:
                pgctl_data = file.read(PG_CONTROL_SIZE)

                self.systemidentifier, = struct.unpack_from('Q', pgctl_data, memoffset['ControlFileData.system_identifier'])
                self.catalog_version_no, = struct.unpack_from('I', pgctl_data, memoffset['ControlFileData.catalog_version_no'])
                self.dbstate, = struct.unpack_from(pgstruct.get_type_format('DBState'), pgctl_data, memoffset['ControlFileData.state'])
                self.checkPoint, = struct.unpack_from('Q', pgctl_data, memoffset['ControlFileData.checkPoint'])
                self.minRecoveryPoint, = struct.unpack_from('Q', pgctl_data, memoffset['ControlFileData.minRecoveryPoint'])
                self.minRecoveryPointTLI, = struct.unpack_from('I', pgctl_data, memoffset['ControlFileData.minRecoveryPointTLI'])
                self.checkPointCopy = CheckPoint(pgctl_data, memoffset['ControlFileData.checkPointCopy'])
                self.xlog_blcksz, = struct.unpack_from('I', pgctl_data, memoffset['ControlFileData.xlog_blcksz'])
                self.xlog_seg_size, = struct.unpack_from('I', pgctl_data, memoffset['ControlFileData.xlog_seg_size'])
                self.blcksz, = struct.unpack_from('I', pgctl_data, memoffset['ControlFileData.blcksz'])
                self.relseg_size, = struct.unpack_from('I', pgctl_data, memoffset['ControlFileData.relseg_size'])
                
                self.nameDataLen, = struct.unpack_from('I', pgctl_data, memoffset['ControlFileData.nameDataLen'])
                
                self.float8ByVal, = struct.unpack_from(pgstruct.get_type_format('bool'), pgctl_data, memoffset['ControlFileData.float8ByVal'])
                self.float4ByVal, = struct.unpack_from(pgstruct.get_type_format('bool'), pgctl_data, memoffset['ControlFileData.float4ByVal'])
                
                self.crc, = struct.unpack_from('I', pgctl_data, memoffset['ControlFileData.crc'])
                
                crcdatas = []
                crcdatas.append((pgctl_data, memoffset['ControlFileData.crc']));
                 
                if not upgcrc.crceq(crcdatas, self.crc):
                    logger.error('pg_control has invalid CRC')
                    raise UPgException('pg_control has invalid CRC')
        except IOError:
            logger.error("Error in reading control file!")
            raise


