import unittest
import logging.config
import shutil

from upgtool.controlfile import ControlFile
from upgtool.upgexception import UPgException
from upgtool import pgstruct
from upgtool import upgcrc

logging.config.fileConfig('cfg/logging.conf')

class TestControlFile(unittest.TestCase):
    def setUp(self):
        pgstruct.load_offset_cfg('cfg/pg95-offset.dat')
        pgstruct.load_type_size('cfg/pg95-type.dat')
        upgcrc.initcrc('lib/pg_crc.so')

    def test_load(self):
        shutil.copy('test-data/pg_control', 'test-data/global/pg_control')
        ctrlfile = ControlFile('test-data');
        self.assertTrue(ctrlfile.systemidentifier == 6145043555394211565) 
        self.assertTrue(ctrlfile.catalog_version_no == 201504291)
        self.assertTrue(ctrlfile.dbstate == 1)
        self.assertTrue(ctrlfile.checkPoint == 301989928)
        self.assertTrue(ctrlfile.minRecoveryPoint == 0)
        self.assertTrue(ctrlfile.minRecoveryPointTLI == 0)
        self.assertTrue(ctrlfile.xlog_blcksz == 8192)
        self.assertTrue(ctrlfile.xlog_seg_size == 16777216)
        self.assertTrue(ctrlfile.blcksz == 8192)
        self.assertTrue(ctrlfile.relseg_size == 131072)
        
        print(ctrlfile.checkPointCopy.time)
        
        self.assertTrue(ctrlfile.checkPointCopy.redo == 301989928);
        self.assertTrue(ctrlfile.checkPointCopy.ThisTimeLineID == 1);
        self.assertTrue(ctrlfile.checkPointCopy.PrevTimeLineID == 1);
        self.assertTrue(ctrlfile.checkPointCopy.fullPageWrites == 1);
        self.assertTrue(ctrlfile.checkPointCopy.nextXidEpoch == 0);
        self.assertTrue(ctrlfile.checkPointCopy.nextXid == 789);
        self.assertTrue(ctrlfile.checkPointCopy.nextOid == 16437);
        self.assertTrue(ctrlfile.checkPointCopy.nextMulti == 1);
        self.assertTrue(ctrlfile.checkPointCopy.nextMultiOffset == 0);
        self.assertTrue(ctrlfile.checkPointCopy.oldestXid == 658);
        self.assertTrue(ctrlfile.checkPointCopy.oldestXidDB == 1);
        self.assertTrue(ctrlfile.checkPointCopy.oldestMulti == 1);
        self.assertTrue(ctrlfile.checkPointCopy.oldestMultiDB == 1);
        self.assertTrue(ctrlfile.checkPointCopy.time == 1434128134);
        self.assertTrue(ctrlfile.checkPointCopy.oldestCommitTs == 0);
        
    def test_crcerror(self):
        shutil.copy('test-data/pg_control_crcerror', 'test-data/global/pg_control')
        try:
            ctrlfile = ControlFile('test-data');
            self.assertTrue(False)
        except UPgException: 
            self.assertTrue(True)
    
if __name__ == '__main__':
    unittest.main