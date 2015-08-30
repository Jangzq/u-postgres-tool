#coding=utf-8 
'''
    封装了scan操作。
    seqscan: 顺序scan。
    
'''

import heapbuf
from tuple import HeapTuple
from upgexception import UPgException

PRO_CONT = 1
PRO_NEXTTUPLE = 2
PRO_SCANFINSH = 3

def seqscan(heapbuffer, relation, startblock, endblock=None, processchain = None):
    assert isinstance(heapbuffer, heapbuf.HeapBuffer) 
    block_num = heapbuffer.getblocknums(relation.relfilenode, heapbuf.MAIN_FORKNUM)
    if not endblock:
        endblock = block_num
    if startblock > block_num:
        return
    if endblock > block_num:
        return
    if startblock >= endblock:
        return
    
    for blockno in range(startblock, endblock):
        ret = scanpage(heapbuffer, relation, blockno, processchain)
        if ret == PRO_SCANFINSH:
            return
    

def scanpage(heapbuffer, relation, blockno, processchain):
    currentpage = heapbuffer.readpage(relation.relfilenode, heapbuf.MAIN_FORKNUM, blockno)
    lines = currentpage.getmaxoffsetnumber()
    for lineoff in range(1, lines+1):
        itemid = currentpage.getitemid(lineoff)
        tuple = HeapTuple(relation, currentpage.data, lineoff, *itemid)
        if processchain:
            retvalue = tuple
            for process in processchain:
                cont, retvalue = process(retvalue)
                if cont == PRO_NEXTTUPLE:
                    break
                if cont == PRO_SCANFINSH:
                    return PRO_SCANFINSH
                
           
class Decode:
    def __init__(self, relation):
        self.relation = relation
         
    def __call__(self, tuple):
        if tuple.relation != self.relation:
            raise UPgException("error in relation scan")
        
        syscolvalue = {}
        syscolvalue['offsetnum'] = tuple.offnum
        syscolvalue['flag'] = tuple.getflagstr()
        
        tupleheader = tuple.gettupleheader()
        syscolvalue['xmin'] = tupleheader.xmin
        syscolvalue['xmax'] = tupleheader.xmax
        syscolvalue['cid'] =  tupleheader.field3
        
        oid = tupleheader.oid if  tupleheader.oid else '' 
        syscolvalue['oid'] = oid
        
        blocknumber = ((tupleheader.ctid_blkid_hi) << 16) | tupleheader.ctid_blkid_lo
        ctid = '(%u,%u)'%(blocknumber, tupleheader.ctid_posid)
        syscolvalue['ctid'] = ctid 

        
        infomaskdesc = self.heaptuple_class.getinfomaskdesc(tupleheader.infomask)
        syscolvalue['infomask'] = tupleheader.infomask
        syscolvalue['infomask_desc'] = infomaskdesc
        infomask2 = self.heaptuple_class.getinfomask2desc(tupleheader.infomask2)
        syscolvalue['infomask2'] = tupleheader.infomask2
        syscolvalue['infomask2_desc'] = infomask2
        
        colvalues = tuple.deformtuple()
       
        return PRO_CONT, (syscolvalue, colvalues)    

class Print:
    def __init__(self, relation):
        self.headerprinted = False
        self.relation = relation
         
    def __call__(self, decoderesult):
        
        syscolvalue, colvalues = decoderesult
        
        if not self.headerprinted:
            self.output.printheader()
            self.headerprinted = True
        self.output.printtuple(syscolvalue, colvalues)
       
        return PRO_CONT, decoderesult
    
    def setoutput(self, output):
        self.output = output
        output.setrelation(self.relation)

class StdOutput:
    def __init__(self, displaysyscol = None, displaycols = None):
        if displaysyscol != None:
            self.displaysyscol = [x.lower() for x in displaysyscol]
        else:
            self.displaysyscol = None
        if displaycols != None:
            self.displaycols = [x.lower() for x in displaycols]
        else:
            self.displaycols = None
        self.formatstr = None
    
    def setrelation(self, relation):
        self.relation = relation
        if self.displaysyscol == None:
            self.displaysyscol = relation.SYS_COLUMNS
        
        if self.displaycols == None:
            self.displaycols = relation.getcolname()
        
        self.formatstr = ''

        if self.displaysyscol:
            for attrname in self.displaysyscol:
                if not relation.hassyscol(attrname):
                    raise UPgException("error in scan output")
                self.formatstr += '|{syscol[%s]:>8}'%(attrname)

        if self.displaycols:
            for attrname in self.displaycols:
                if not relation.hascol(attrname):
                    raise UPgException("error in scan output")
                self.formatstr += '|{col[%s]:>8}'%(attrname)
        
    
    def printheader(self):
        sysheader = {}
        for attrname in self.displaysyscol:
            sysheader[attrname] = attrname
        colheader = {}
        for attrname in self.displaycols:
            colheader[attrname] = attrname
        
        print self.formatstr.format(syscol=sysheader, col=colheader)
    
    def printtuple(self, syscolvalues, colvalues):
        print self.formatstr.format(syscol=syscolvalues, col = colvalues)
        
        
