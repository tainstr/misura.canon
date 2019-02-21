# -*- coding: utf-8 -*-
from misura.canon.logger import get_module_logging
logging = get_module_logging(__name__)
import re
from time import time

coalesce_chron = 2 # seconds
def manage_chron(obj, key, nval):
    if not obj.hasattr(key, 'chron'):
        obj.setattr(key, 'chron', [[],[]])
    chron = obj.getattr(key, 'chron')
    chron[0].append(time())
    chron[1].append(nval)
    if len(chron[0])>2 and chron[0][-1]-chron[0][-2]<coalesce_chron:
        chron[0].pop(-2)
        chron[1].pop(-2)
    if len(chron)>20:
        chron[0].pop(0)
        chron[1].pop(0)
    obj.setattr(key, 'chron', chron)
    return chron

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

    
def resolve_role(obj, opt):
    if not opt['type'].startswith('Role'):
        return obj, False
    pt = opt['options'][:]
    pt.append(False)
    path, preset, io = pt[:3]
    if path in (False, None, 'None'):
        return False, False
    obj = obj.root.toPath(path)
    if obj is None:
        return False, False
    if io:
        io = obj.gete(io)
    return obj, io

        
def scan_option(obj, keys, out=False):
    """Return a dictionary containing all possible object paths 
    containing an option named key and its current value"""
    out= out or {}
    
    vals = {}
    for key in keys:
        if key in obj:
            vals[key] = obj[key]
        else:
            break
        
    if len(vals)==len(keys): 
        out[obj['fullpath']] = vals
        
    for sub in obj.devices:
        if sub==obj:
            continue
        out = scan_option(sub, keys, out)
    return out

class CommonProxy(object):
    separator = '/'
    _parent = False
    _readLevel = 5
    _writeLevel = 5
    _rmodel = False
    """Cached remote recursive model dictionary"""
    _recursiveModel = False
    """Cached Item model for tree representation"""
    _navigator = None
    """Navigator instance for configuration-plot interactions"""
    _doc = False
    _changeset = 0
    
    def __nonzero__(self):
        return 1

    def get(self, *a, **k):
        return self.__getitem__(*a, **k)
    
    def set(self, *a, **k):
        return self.__setitem__(*a, **k)
        
    
    def dump_model(self):
        self._rmodel = False
        self._recursiveModel = False
        self.root._rmodel = False
        self.root._recursiveModel = False
    
    @property
    def instrument_obj(self):
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
        
    def plain_kid(self, opt):
        kid = self.getattr(opt, 'kid')
        for c in '/:;,-=+#@.\\':
            kid = kid.replace(c,'_')
        return kid
        
    def asdict(self):
        """Return all keys current values in a dictionary"""
        r = {}
        for k in self.keys():
            r[k] = self[k]
        return r
    
    def resolve_role(self, key):
        opt = self.gete(key)
        return resolve_role(self, opt)
    
    def is_live(self):
        """Returns if the proxy refers to a remotely connected object or to a local data structure"""
        return False
    
    def iter_parents(self):
        """Yield parent objects until root is found"""
        conf = self
        while conf:
            conf = conf.parent()
            if conf:
                yield conf
    
    def search_parent_key(self, dkey, default=None, reverse=False):
        """Recursively search dkey in itself and all parents options"""
        conf = self
        
        # Create the reverse stack
        if reverse in (True, 1):
            reverse = [self]+list(self.iter_parents())
        # Walk the reverse stack until the required key is found, or default is returned
        if reverse not in (False, 0):
            if len(reverse)==0:
                # End of the stack: give up
                return default
            conf = reverse.pop(-1)
            if dkey in conf:
                return conf[dkey]
            return conf.search_parent_key(dkey, default=default, reverse=reverse)
            
        if dkey not in conf:
            if not conf.parent():
                #logging.debug('Parent key not found', conf['fullpath'], dkey)
                return default
            return conf.parent().search_parent_key(dkey, default=default)
        return conf[dkey]

        
        
        
        

    
    
