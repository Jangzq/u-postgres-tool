#coding=utf-8 
'''
    在u-postgres-tool中，使用LRU管理heap buffer，
'''
import ctypes
from upgexception import UPgException
from controlfile import ControlFile

'''
    LRU page，成员变量data保存page数据， key是此page的key。
'''
class LruPage:
    def __init__(self, pagesz):
        self.pagesz = pagesz
        self.data = ctypes.create_string_buffer(pagesz)
        self.dirty = False
        self.key = None
    
    def load_data(self, key, data):
        self.key = key
        ctypes.memmove(self.data, data, self.pagesz)

'''
    LRU cache。
    在构造函数中，需要传入page的类，以及page的size，以及缓存的page个数。
    在构造函数中，初始化需要缓存的page，并且连成链表。
    load新的page时，首先选择空闲的page，如果没有使用最老的page，并且使用dict: keys
    来加速查找。
'''
class LruCache:
    def __init__(self, pageclass, pagesz, pagecnt):
        self.head = None
        self.pagesz = pagesz
        self.keys = {}
        for i in range(pagecnt):
            page = pageclass(pagesz)
            if not self.head:
                self.head = page
                page.prev = page
                page.next = page
            else:
                self.__append(page)
        
    def __append(self, page):
        self.head.prev.next = page
        page.prev = self.head.prev
        page.next = self.head
        self.head.prev = page

    def visit(self, page):
        page.prev.next = page.next
        page.next.prev = page.prev
        if page is self.head:
            self.head = page.next
        self.__append(page)

    def __get_replace_page(self):
        if self.head.dirty:
            self.head.writeback()
        return self.head
    
    def get(self, key):
        try:
            return self.keys[key]
        except:
            return None
  
    def get_and_visit(self, key):
        ret = self.get(key)
        if ret:
            self.visit(ret)
        return ret
        
    def put(self, key, data):
        if len(data) != self.pagesz:
            raise UPgException("data size should be %u instead of %u"%(self.pagesz, len(data)))
        page = self.get(key)
        if not page:
            page = self.__get_replace_page()
            if page.key:
                self.keys.pop(page.key)
        page.load_data(key, data)
        self.keys[key] = page
        self.visit(page)
        return page


