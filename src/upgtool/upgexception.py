#coding=utf-8 

class UPgException(Exception):
    def __init__(self, error_desc):
        Exception.__init__(self)
        self.error_desc = error_desc