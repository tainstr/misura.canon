# -*- coding: utf-8 -*-
"""Option persistence."""
from .. import logger
from option import Option, read_only_keys
from store import Store
from option import ao

logging = logger.get_module_logging(__name__)

class Conf(object):
    kid_base = ''

    def __init__(self, desc=False, empty=False):
        self.empty = empty
        self.log = logger.Log
        self.desc = {}
        if desc is not False:
            for k, v in desc.iteritems():
                if not v.has_key('handle'):
                    v['handle'] = k
                if isinstance(v, dict):
                    v = Option(**v)
                self.desc[k] = v
                
    def __contains__(self, *a, **k):
        return self.has_key(*a, **k)

    def iteritems(self):
        return self.desc.iteritems()

    def iterkeys(self):
        return self.desc.iterkeys()

    def itervalues(self):
        return self.desc.itervalues()
    
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
#       print 'option.Conf.get', name, a
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
            if not self.has_key(name):
                self.sete(name, {'factory_default': nval})
                print 'SET EMPTY',name,nval
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
        return self.desc.has_key(key)

    def keys(self):
        return self.desc.keys()

    def gete(self, name):
        """Returns description dictionary for option `name`"""
        return self.desc[name]

    def sete(self, name, opt):
        """Sets option `name` to dictionary or Option `opt`."""
        if not opt.has_key('type'):
            opt['type'] = 'Empty'
        if not opt.has_key('handle'):
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
        if kwargs.has_key('overwrite'):
            overwrite = kwargs.pop('overwrite')
        ao(out, *args, **kwargs)
        out = out.values()[0]
        key = out['handle']
        if out['priority'] == 0:
            out['priority'] = -1
        # If option was already defined, update old one with new values
        out = Option(**out)
        if self.has_key(key):
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

    def setattr(self, handle, key, val):
        """Sets to val the `key` of `handle` option"""
        if key in read_only_keys:
            print 'Attempt to modify read-only key'
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
        if not self.desc.has_key(name):
            return False
        del self.desc[name]
        return True

    # FIXME: Check if all these key-management methods has still some use...
    def setFlags(self, opt, upDict):
        """Update flags for `opt` with `upDict`."""
        d = self.desc[opt]
        if not d.has_key('flags'):
            return False
        d['flags'].update(upDict)
        self.sete(opt, d)
        return True

    def getFlags(self, opt):
        if not self.desc[opt].has_key('flags'):
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
            return False
        e['attr'].append(attr_name)
        self.sete(opt, e)
        return True
    
    def del_attr(self, opt, attr_name):
        """Remove `attr_name` from the attr list of keyword, if found"""
        e = self.gete(opt)
        if attr_name not in e['attr']:
            return False
        e['attr'].remove(attr_name)
        self.sete(opt, e)
        return True    

    def validate(self):
        """Verify current configuration and updates KID"""
        opt = Store(kid=self.kid_base)
        opt.desc = self.desc
        failed = opt.validate()
        if len(failed) > 0:
            print 'Failed validations', failed
        for k, v in opt.desc.iteritems():
            self.desc[k] = v
        return failed

    def iolist(self):
        """Returns a list of options having History attribute set or RoleIO type."""
        r = []
        for opt in self.desc.itervalues():
            # io can point only towards History or RoleIO types
            if 'History' in opt['attr'] or opt['type'] == 'RoleIO':
                r.append(opt['handle'])
        return r
    
