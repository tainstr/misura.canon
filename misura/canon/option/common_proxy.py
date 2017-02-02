# -*- coding: utf-8 -*-
from misura.canon.logger import get_module_logging
logging = get_module_logging(__name__)
import re

def from_column(column, proxy=False):
    """Returns the object able to return the column `col` and the column option name"""
    column = re.sub("^[0-9]+:", '', column)
    column = re.sub("^/ver_[0-9]+", '', column)
    column = re.sub("^/summary/", '', column)
    column = re.sub("^/", '', column)
    branches = column.split('/')
    
    if proxy is False:
        return branches
    
    name = branches.pop(-1)
    obj = proxy.toPath(branches)
    return obj, name


class CommonProxy(object):
    separator = '/'
    _parent = False
    _readLevel = 5
    _writeLevel = 5
    _rmodel = False
    """Cached remote recursive model dictionary"""
    _navigator = None
    """Navigator instance for configuration-plot interactions"""
    _doc = None
    
    @property
    def instrument(self):
        root = self.root 
        ins = root['runningInstrument']
        ins = getattr(root, ins, False)
        return ins
    
    @property
    def navigator(self):
        return self.root._navigator
    
    @navigator.setter
    def navigator(self, nav):
        self.root._navigator = nav
    
    @property
    def doc(self):
        return self.root._doc 
    
    @doc.setter
    def doc(self, doc):
        self.root._doc = doc
    
    
