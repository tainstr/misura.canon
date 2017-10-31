# -*- coding: utf-8 -*-
"""Option persistence."""
import re
import collections
from threading import Lock
from functools import cmp_to_key
from ..csutil import lockme
from .. import logger
from .conf import Conf
try:
    import cPickle as pickle
except:
    import pickle
from ..milang import Scriptable
from .aggregative import Aggregative
from . import common_proxy


logging = logger.get_module_logging(__name__)


def dictRecursiveModel(base):
    """Build a dictionary configuration tree from ConfigurationProxy `base`"""
    out = collections.OrderedDict()
    for path, obj in base.items():
        if path == 'self':
            out[path] = obj['name']
            continue
        out[path] = dictRecursiveModel(obj)
    return out


def print_tree(tree, level=0, current=False):
    """Pretty-print a dictionary configuration `tree`"""
    pre = '   ' * level
    msg = ''
    for k, v in tree.items():
        if k == 'self':
            msg += print_tree(v, level)
            continue
        # Detect subdevice
        if isinstance(v, dict) and 'self' in v:
            msg += pre + '|++> ' + k + '\n'
            msg += print_tree(v, level + 1)
            continue
        if not current:
            continue
        v = repr(v['current'])
        if len(v) > 50:
            v = v[:46] + ' ...'
        msg += '{}|: {} = {}\n'.format(pre, k, v)
    return msg


class ConfigurationProxy(common_proxy.CommonProxy, Aggregative, Scriptable, Conf):
    """A configuration object behaving like a live server"""
    callbacks_get = set()
    callbacks_set = set()
    filename = False  # Filename from which this configuration was red

    def print_tree(self, *a, **k):
        print(print_tree(self.tree(), *a, **k))

    def __init__(self, desc=False, name='MAINSERVER', parent=False, readLevel=-1, writeLevel=-1, kid_base='/'):
        self._lock = Lock()
        if not desc:
            desc = collections.OrderedDict({'self': {}})
        Scriptable.__init__(self)
        self.log = logging
        self.kid_base = kid_base
        Conf.__init__(self, desc['self'])
        if readLevel > 0:
            self._readLevel = readLevel
        if writeLevel > 0:
            self._writeLevel = writeLevel
        self.children = desc #.copy()
        """Child configuration dictionaries"""
        self.children_obj = {}
        """Instantiated children configuration proxies"""
        # Remove myself from children...
        del self.children['self']
        self.name = name
        self._parent = parent
        if not parent:
            self._Method__name = name
        else:
            self._Method__name = parent._Method__name + self.separator + name
#           if parent._parent:
#               self._Method__name=parent._Method__name+self.separator+name
#           else:
#               self._Method__name=name
        if 'devpath' in self:
            self['devpath'] = name
        self.autosort()
        

    def __getstate__(self):
        result = self.__dict__.copy()
        result.pop('children_obj')
        # These might contain unpickable references
        result.pop('callbacks_get', 0)
        result.pop('callbacks_set', 0)
        result.pop('_navigator', 0)
        result.pop('_doc', 0)
        result.pop('_lock',0)
        return result

    def __setstate__(self, state):
        self.__dict__ = state
        self.children_obj = {}
        self.callbacks_set = self.__class__.callbacks_set
        self.callbacks_get = self.__class__.callbacks_get
        self._navigator = None
        self._doc = None
        self._lock = Lock()

    @property
    def root(self):
        if not self._parent:
            return self
        return self._parent.root

    def get_fullpath(self):
        po = self
        path = []
        while True:
            if po.name == 'MAINSERVER':
                break
            path.append(po.name)
            po = po.parent()
            if po in [None, False]:
                break
        # Revert to get the top-down path
        path.reverse()
        r = '/' + '/'.join(path) + '/'
        self['fullpath'] = r
        return r

    def _update_from_children(self):
        """ Retrieve current configuration from instantiated child objects"""
        for key, obj in self.children_obj.items():
            obj.get_fullpath()
            d = {'self': obj.desc}
            obj._update_from_children()
            d.update(obj.children)
            self.children[key] = d
        self.autosort()

    def rmodel(self):
        out = dictRecursiveModel(self.children)
        out['self'] = self.name
        return out

    def tree(self):
        """Build a full configuration tree suitable for serialization and saving on a file"""
        self._update_from_children()
        tree = self.children.copy()
        tree['self'] = self.desc.copy()
        return tree

    def __len__(self):
        return len(self.desc)

    def iteritems(self):
        for i in self.desc.items(): 
            yield i 
            
    def items(self):
        return self.desc.items()

    def itervalues(self):
        for v in self.desc.values():
            yield v

    def values(self):
        return self.desc.values()

    def iterkeys(self):
        for k in self.desc.keys():
            yield k
            
    def keys(self):
        return self.desc.keys()

    def has_key(self, k):
        return k in self.desc
    
    def __contains(self, k):
        return k in self.desc

    def dumps(self):
        d = {'self': self.desc.copy()}
        self._update_from_children()
        d.update(self.children)
        return pickle.dumps(d)

    def describe(self, *a, **kw):
        return self.desc

    def connect(self, *a, **k):
        pass

    def connection(self, *a, **k):
        return True

    def paste(self, obj):
        self.desc = obj.desc.copy()
        self.children = obj.children #.copy()
        self.children_obj = obj.children_obj #.copy()
        self._parent = obj._parent
        self._Method__name = obj._Method__name
        self._readLevel = obj._readLevel
        self._writeLevel = obj._writeLevel
        self._navigator = obj._navigator
        self._doc = obj._doc
        self.filename = obj.filename

    def copy(self):
        p = ConfigurationProxy()
        p.paste(self)
        return p

    def __nonzero__(self):
        return 1

    def get(self, *a, **k):
        return self.__getitem__(*a, **k)

    def set(self, *a, **k):
        return self.__setitem__(*a, **k)

    def __getitem__(self, key, *a):
        if key == 'fullpath':
            return self.get_fullpath()
        if len(a) == 1 and key not in self.desc:
            return a[0]
        old = self.desc[key]['current']
        new = self.callback(key, old, callback_name='get')
        return new

    def callback(self, key, val, callback_name='set'):
        callback_group = getattr(self, 'callbacks_' + callback_name)
        for cb in callback_group:
            old = self.desc[key]['current']
            val = cb(self, key, old, val)
        return val

    def __setitem__(self, key, val):
        if key not in self.desc:
            self.log.error('Impossible to set non-existing option', key, val)
            return False
        if self.desc[key].get('writeLevel', 0) > self._writeLevel:
            self.log.error(
                'No authorization to edit the option', key, self._writeLevel)
            return False
        self.desc[key]['current'] = self.callback(key, val)
        return True

    def gettype(self, key):
        return self.desc[key]['type']

    def multiget(self, keys):
        r = {}
        for k in keys:
            r[k] = self[k]
        return r

    def __getattr__(self, path):
        if path.startswith('_'):
            return object.__getattribute__(self, path)
        elif path in dir(self):
            return object.__getattribute__(self, path)
        return self.child(path)

    def gete(self, key):
        return self.desc[key]
    
    def __delitem__(self, key):
        del self.desc[key]

    def sete(self, key, val):
        if 'priority' not in val or val['priority'] == -1:
            priorities = [opt.get('priority', 0) for opt in self.desc.values()]
            if len(priorities):
                val['priority'] = max(priorities) + 1
        old = self.desc.get(key, val)
        if old.get('writeLevel', 0) > self._writeLevel:
            self.log.error(
                'No authorization to edit the option', key, self._writeLevel)
            return False
        self.desc[key] = val
        return True

    def autosort(self):
        def sorter(item, item2):
            key, val = item
            digits = re.sub(r"\D", '', key)
            key = int(digits) if len(digits) else key
            digits = re.sub(r"\D", '', item2[0])
            key2 = int(digits) if len(digits) else item2[0]
            if type(key)==type(key2):
                return (key>key2) or -1
            elif isinstance(key, int):
                return 1
            return -1
        self.children = collections.OrderedDict(
            sorted(self.children.items(), key=cmp_to_key(sorter)))
        self.dump_model()
    
    @lockme()
    def add_child(self, name, desc, overwrite=False):
        """Inserts a sub-object `name` with object tree dictionary `desc`.
        Returns a ConfigurationProxy to the new child."""
        # If desc is the description dictionary and not the object tree dictionary,
        # encapsulate it in a 'self' dict
        if not 'self' in desc:
            desc = {'self': desc}
        if overwrite or (name not in self.children):
            self.children[name] = desc
        else:
            for key, val in desc['self'].items():
                self.children[name]['self'][key] = val
        # Remove stale instantiated CP child
        self.children_obj.pop(name, False)
        self.autosort()
        # Notify tree about the new object
        self.root._update_from_children()
        # Recreate child CP instance
        r = self.child(name)
        # Autocalc path
        r.get_fullpath()
        return r

    def getFlags(self, opt):
        if 'flags' not in self.desc[opt]:
            return {}
        return self.desc[opt]['flags']

    def list(self):
        return [(c, c) for c in self.children.keys()]

    def has_child(self, name):
        """Returns if `name` is a child"""
        return name in self.children

    def child(self, name):
        """Return child ConfigurationProxy object by `name`"""
        if name not in self.children:
            return None
        if name not in self.children_obj:
            kb = self.kid_base + name + self.separator
            obj = ConfigurationProxy(self.children[name],
                                     name=name, parent=self, kid_base=kb,
                                     readLevel=self._readLevel, writeLevel=self._writeLevel)
            obj.filename = self.filename
            self.children_obj[name] = obj
        return self.children_obj[name]
    

    @property
    def devices(self):
        return [self.child(name) for name in self.children.keys()]

    def parent(self):
        return self._parent

    def toPath(self, lst):
        """Returns a copy of the object at the path expressed in list lst"""
        if isinstance(lst, basestring):
            lst = lst.split(self.separator)
            while lst[0] in ('server', ''):
                lst.pop(0)
            while lst[-1] == '':
                lst.pop(-1)
        obj = self.copy()
        for p in lst:
            obj = obj.child(p)
            if obj is None:
                logging.error('toPath: Cannot find child path', p, lst)
                return None
        return obj

    def from_column(self, col0):
        return common_proxy.from_column(col0, self.root)

    def toMethodName(self, name):
        if name == 'MAINSERVER':
            return
        lst = name.split(self.separator)
        return self.toPath(lst)

    def searchPath(self, path):
        """Search a child with the corresponding devpath=`path` and returns its name"""
        fp = self.get_fullpath().replace('//', '/')
        if not path.startswith(fp):
            print('searchPath: not corresponding!', path, fp)
            return False
        # Cut itself from the path
        if not path.endswith('/'):
            path += '/'
        vpath = path[len(fp):].split('/')[:-1]

        # Verify existence
        obj = self
        for p in vpath:
            obj = obj.child(p)
            if obj is None:
                print('searchPath: not found', repr(p), repr(path))
        return self.separator.join(vpath)

    def listPresets(self):
        return ['default']

    def iterprint(self, base=False, pre=''):
        """Iterative printing for debug purposes"""
        if not base:
            base = self
        for k, v in base.items():
            print(pre, k, v['current'])
        for sub in base.children.keys():
            base.child(sub).iterprint(pre=pre + '\t%s: ' % sub)

    @property
    def samples(self):
        if 'nSamples' not in self.measure:
            self.log.debug('no nSample option!')
            return []
        n = self.measure['nSamples']
        if n == 0:
            self.log.debug('no samples defined!')
            return []
        out = []
        for i in range(n + 2):
            # Search direct child (instrument)
            child = self.child('sample{}'.format(i))
            if child:
                out.append(child)
                continue
            # Search referred sample (from device)
            s = 'smp{}'.format(i)
            if s not in self:
                self.log.debug('sample not found', s)
                continue
            # Get fullpath of the referred sample object
            s = self[s][0]
            # Get the actual object
            obj = self.root.toPath(s)
            if obj is None:
                self.log.debug('sample object not found', s)
                continue
            out.append(obj)
        self.log.debug('returning samples', out, n)
        return out

    def role2dev(self, opt):
        """Return the device object associated with role option `opt`"""
        p = self[opt]
        if p is False:
            return False
        p = p[0]
        if p in ('None', None):
            return False
        obj = self.root.toPath(p)
        if not obj:
            return False
        return obj

    def check_read(self, opt):
        return self.desc[opt].get('readLevel', 0) <= self._readLevel

    def check_write(self, opt):
        return self.desc[opt].get('writeLevel', 0) <= self._writeLevel
    
    def compare_option(self, *keys):
        r = common_proxy.scan_option(self.root, keys)
        return r
