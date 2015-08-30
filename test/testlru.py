from upgtool import lru
import unittest
import ctypes

class Page(lru.LruPage):
    def load_data(self, key, data):
        lru.LruPage.load_data(self, key, data)

class TestLru(unittest.TestCase):
    
    def test_lru(self):
        cache = lru.LruCache(Page, 5, 3)
        cache.put(1, 'a1234')
        self.assertTrue(len(cache.keys) == 1)
        self.assertTrue(isinstance(cache.head, Page))
        self.assertTrue(ctypes.string_at(cache.head.prev.data, 5) == 'a1234')
        self.assertTrue(cache.get(1) is cache.head.prev)
        
        cache.put(2, 'b1234')
        self.assertTrue(len(cache.keys) == 2)
        self.assertTrue(ctypes.string_at(cache.head.prev.data, 5) == 'b1234')
        self.assertTrue(cache.get(2) is cache.head.prev)
        self.assertTrue(cache.get(1).next is cache.get(2))
        self.assertTrue(cache.get(2).next is cache.head)
        self.assertTrue(cache.head.next is cache.get(1))
        
        cache.put(3, 'c1234')
        self.assertTrue(len(cache.keys) == 3)
        self.assertTrue(ctypes.string_at(cache.head.prev.data, 5) == 'c1234')
        self.assertTrue(cache.get(3) is cache.head.prev)
        self.assertTrue(cache.get(2).next is cache.get(3))
        self.assertTrue(cache.get(3).next is cache.head)
        self.assertTrue(cache.head is cache.get(1))
        
        cache.put(4, 'd1234')
        self.assertTrue(len(cache.keys) == 3)
        self.assertTrue(cache.keys.keys() == [2,3,4])
        self.assertTrue(not cache.get(1))
        self.assertTrue(ctypes.string_at(cache.get(2).data, 5) == 'b1234')
        self.assertTrue(ctypes.string_at(cache.get(3).data, 5) == 'c1234')
        self.assertTrue(ctypes.string_at(cache.get(4).data, 5) == 'd1234')
        self.assertTrue(cache.head is cache.get(2))
        
        cache.get_and_visit(2)
        self.assertTrue(ctypes.string_at(cache.get(2).data, 5) == 'b1234')
        self.assertTrue(ctypes.string_at(cache.get(3).data, 5) == 'c1234')
        self.assertTrue(ctypes.string_at(cache.get(4).data, 5) == 'd1234')
        self.assertTrue(cache.head is cache.get(3))
        
        

#         self.assertTrue(cache.head is cache.keys[1])
        
#         cache.put(2, 'b1234')
#         cache.put(3, 'c1234') 
#         cache.put(4, 'd1234')
#         page = cache.get(2)
#         cache.visit(page)
#         cache.put(5, 'e1234')
#         print cache.keys       
        


if __name__ == '__main__':
    unittest.main