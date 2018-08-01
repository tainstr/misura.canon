# -*- coding: utf-8 -*-
"""Option persistence."""
from .. import logger
from .option import Option, read_only_keys
from .store import Store
from .option import ao

try:
    unicode('a')
except:
    unicode=str

logging = logger.get_module_logging(__name__)

class Conf(object):
    kid_base = ''

    def __init__(self, desc=False, empty=False):
        self.empty = empty
        #FIXME: replace with logging
        self.log = logger.Log
        self.desc = {}
        if desc is not False:
            for k, v in desc.items():
                if 'handle' not in v:
                    v['handle'] = k
                if isinstance(v, dict):
                    v = Option(**v)
                self.desc[k] = v
                
    def __contains__(self, *a, **k):
        return self.has_key(*a, **k)
    
    def items(self):
        return self.desc.items()

    def iteritems(self):
        for item in self.desc.items():
            yield item
            
    def keys(self):
        return self.desc.keys()

    def iterkeys(self):
        for key in self.desc.keys():
            yield key

    def itervalues(self):
        for val in self.desc.values():
            yield val
    
    def values(self):
        return self.desc.values()

    def listPresets(self):
        return []

    def close(self):
        return True

    def setEmpty(self, val):
        """Empty mode. If true, any `set` call creates a corresponding empty option. 
        Usually false, returns an error."""
        if val:
            self.empty = True
        else:
            self.empty = False

    def getEmpty(self):
        return self.empty

    def __getitem__(self, key):
        return self.get(key)

    def get_current(self, name):
        """Actually get the current value for name"""
        return self.desc.get(name)['current']

    def get(self, name, *a):
        """Return option `name` current value"""
        try:
            opt = self.get_current(name)
        except:
            if len(a) > 0:
                return a[0]
            else:
                raise
        return opt

    def __setitem__(self, key, val):
        return self.set(key, val)

    def set_current(self, name, nval, **k):
        """Actually set the value for name"""
        d = self.desc[name]
        oval = d['current']
        d['current'] = nval
        self.desc.set(name, d, **k)
        return nval

    def set(self, name, nval, **k):
        """Set the current value of a key"""
        if self.empty:
            if name not in self:
                self.sete(name, {'factory_default': nval})
                print('SET EMPTY',name,nval)
        r = self.set_current(name, nval, **k)
        return r
    
    def coerce(self, name, value):
        """Conform `value` type to option `name` type before setting it"""
        t = self.desc[name]['type']
        if t=='Float':
            value = float(value)
        elif t=='Integer':
            value = int(value)
        elif t in ('String','TextArea'):
            value = unicode(value)
        return self.set(name, value)

    def has_key(self, key):
        """Check  `key`"""
        # Warning: slow on server side!
        return key in self.desc
    
    def gete(self, name):
        """Returns description dictionary for option `name`"""
        return self.desc[name]

    def sete(self, name, opt):
        """Sets option `name` to dictionary or Option `opt`."""
        if 'type' not in opt:
            opt['type'] = 'Empty'
        if 'handle' not in opt:
            opt['handle'] = name
        if isinstance(opt, dict):
            opt = Option(**opt)
            opt.set_base_kid(self.kid_base)
        self.desc[name] = opt
        return True
    
    def add_option(self, *args, **kwargs):
        """Creates a new option using the ao() utility function. 
        Overwrite old one if existing."""
        out = {}
        overwrite = True
        if 'overwrite' in kwargs:
            overwrite = kwargs.pop('overwrite')
        ao(out, *args, **kwargs)
        out = list(out.values())[0]
        key = out['handle']
        if out['priority'] == 0:
            out['priority'] = -1
        # If option was already defined, update old one with new values
        out = Option(**out)
        if key in self:
            # Do not do anything if not overwriting
            if not overwrite:
                return out
            origin = self.gete(key)
            origin.migrate_from(out)
            out = origin
        
        self.sete(key, out)
        return out

    def getattr(self, handle, attr):
        """Returns the attribute `attr` of an option `name`"""
        return self.desc[handle][attr]
    
    def hasattr(self, handle, attr):
        if not self.has_key(handle):
            return False
        return attr in self.desc[handle]

    def setattr(self, handle, key, val):
        """Sets to val the `key` of `handle` option"""
        if key in read_only_keys:
            print('Attempt to modify read-only key', handle, key, val)
            return False
        opt = self.desc[handle]
        opt[key] = val
        self.desc[handle] = opt
        return True

    def getkid(self, name):
        return self.desc[name]['kid']

    def gettype(self, name):
        return self.desc[name]['type']

    def delete(self, name):
        """Remove key `name`"""
        # FIXME: verifica!!! del_key!???
        if name not in self.desc:
            return False
        del self.desc[name]
        return True

    # FIXME: Check if all these key-management methods has still some use...
    def setFlags(self, opt, upDict):
        """Update flags for `opt` with `upDict`."""
        d = self.desc[opt]
        if 'flags' not in d:
            return False
        d['flags'].update(upDict)
        self.sete(opt, d)
        return True

    def getFlags(self, opt):
        if 'flags' not in self.desc[opt]:
            return {}
        return self.desc[opt]['flags']

    def getAttributes(self, name):
        return self.desc[name]['attr']

    def setAttributes(self, name, attrlist):
        e = self.gete(name)
        e['attr'] = attrlist
        self.sete(name, e)
        return True
    
    def add_attr(self, opt, attr_name):
        """Add `attr_name` to the attr list of keywords, if missing"""
        e = self.gete(opt)
        if attr_name in e['attr']:
            self.log.debug('Cannot add found attribute', opt, attr_name)
            return False
        e['attr'].append(attr_name)
        self.sete(opt, e)
        self.log.debug('Added attribute', opt, attr_name)
        return True
    
    def del_attr(self, opt, attr_name):
        """Remove `attr_name` from the attr list of keyword, if found"""
        e = self.gete(opt)
        if attr_name not in e['attr']:
            self.log.debug('Cannot delete missing attribute', opt, attr_name)
            return False
        e['attr'].remove(attr_name)
        self.sete(opt, e)
        self.log.debug('Deleted attribute', opt, attr_name)
        return True    

    def validate(self):
        """Verify current configuration and updates KID"""
        opt = Store(kid=self.kid_base)
        opt.desc = self.desc
        failed = opt.validate()
        if len(failed) > 0:
            print('Failed validations', failed)
        for k, v in opt.desc.iteritems():
            self.desc[k] = v
        return failed

    def iolist(self):
        """Returns a list of options having History attribute set or RoleIO type."""
        r = []
        for opt in self.desc.values():
            # io can point only towards History or RoleIO types
            if 'History' in opt['attr'] or opt['type'] == 'RoleIO':
                r.append(opt['handle'])
        return r
    
