# -*- coding: utf-8 -*-
"""Option persistence."""
from .. import logger
from conf import Conf
import cPickle as pickle
from ..milang import Scriptable


def dictRecursiveModel(base):
    """BUild a dictionary configuration tree from ConfigurationProxy `base`"""
    out = {}
    for path, obj in base.iteritems():
        if path == 'self':
            out[path] = obj['name']
            continue
        out[path] = dictRecursiveModel(obj)
    return out


def print_tree(tree, level=0):
    """Pretty-print a dictionary configuration `tree`"""
    pre = '   ' * level
    msg = ''
    for k, v in tree.iteritems():
        if k == 'self':
            msg += print_tree(v, level)
            continue
        # Detect subdevice
        if isinstance(v, dict) and 'self' in v:
            msg += pre + '|++> ' + k + '\n'
            msg += print_tree(v, level + 1)
            continue
        v = repr(v['current'])
        if len(v) > 50:
            v = v[:46] + ' ...'
        msg += '{}|: {} = {}\n'.format(pre, k, v)
    return msg


class ConfigurationProxy(Scriptable, Conf):

    """A configuration object behaving like a live server"""
    separator = '/'
    _readLevel = 5
    _writeLevel = 5

    def __init__(self, desc={'self': {}}, name='MAINSERVER', parent=False, readLevel=5, writeLevel=5, kid_base='/'):
        Scriptable.__init__(self)
        self.log = logger.BaseLogger()
        self.kid_base = kid_base
        Conf.__init__(self, desc['self'])
        self._readLevel = readLevel
        self._writeLevel = writeLevel
        self.children = desc.copy()
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
# 			if parent._parent:
# 				self._Method__name=parent._Method__name+self.separator+name
# 			else:
# 				self._Method__name=name
        self.get = self.__getitem__
        self.set = self.__setitem__

    def check_read(self, opt):
        return self.desc[opt]['readLevel'] <= self._readLevel

    def check_write(self, opt):
        return self.desc[opt]['writeLevel'] <= self._writeLevel

    @property
    def root(self):
        if not self._parent:
            return self
        return self._parent.root

    @property
    def naturalName(self):
        return self.name

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
        return '/' + '/'.join(path) + '/'

    def _update_from_children(self):
        """ Retrieve current configuration from instantiated child objects"""
        for key, obj in self.children_obj.iteritems():
            d = {'self': obj.desc}
            obj._update_from_children()
            d.update(obj.children)
            self.children[key] = d

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
        return self.desc.iteritems()

    def itervalues(self):
        return self.desc.itervalues()

    def iterkeys(self):
        return self.desc.iterkeys()

    def has_key(self, k):
        return self.desc.has_key(k)

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
        self.children = obj.children.copy()
        self.children_obj = obj.children_obj
        self._parent = obj._parent
        self._Method__name = obj._Method__name
        self._readLevel = obj._readLevel
        self._writeLevel = obj._writeLevel

    def copy(self):
        p = ConfigurationProxy()
        p.paste(self)
        return p

    def __nonzero__(self):
        return 1

    def __getitem__(self, key):
        if key == 'fullpath':
            return self.get_fullpath()
        return self.desc[key]['current']

    def __setitem__(self, key, val):
        if not self.desc.has_key(key):
            print 'Impossible to set key', key, val
            return False
        self.desc[key]['current'] = val
        return True

    def gettype(self, key):
        return self.desc[key]['type']

    def multiget(self, keys):
        r = {}
        for k in keys:
            r[k] = self[k]
        return r

    def __getattr__(self, path):
        if path in ['__methods__', '__members__', '_Method__name']:
            return object.__getattribute__(self, path)
        elif path in dir(self):
            return object.__getattribute__(self, path)
        return self.child(path)

    def gete(self, key):
        return self.desc[key]

    def sete(self, key, val):
        self.desc[key] = val

    def getFlags(self, opt):
        if not self.desc[opt].has_key('flags'):
            return {}
        return self.desc[opt]['flags']

    def list(self):
        return [(c, c) for c in self.children.iterkeys()]

    def has_child(self, name):
        """Returns if `name` is a child"""
        return self.children.has_key(name)

    def child(self, name):
        """Return child ConfigurationProxy object by `name`"""
        if not self.children.has_key(name):
            return None
        if not self.children_obj.has_key(name):
            kb = self.kid_base + name + self.separator
            self.children_obj[name] = ConfigurationProxy(
                self.children[name],	name=name, parent=self, kid_base=kb)
        return self.children_obj[name]

    def parent(self):
        return self._parent

    def toPath(self, lst):
        """Returns a copy of the object at the path expressed in list lst"""
        if isinstance(lst, str):
            lst = lst.split(self.separator)
            if lst[0] in ['server', '']:
                lst.pop(0)
            if lst[-1] == '':
                lst.pop(-1)
        obj = self.copy()
        for p in lst:
            if obj is None:
                return None
            obj = obj.child(p)
        return obj

    def toMethodName(self, name):
        if name == 'MAINSERVER':
            return
        lst = name.split(self.separator)
        return self.toPath(lst)

    def searchPath(self, path):
        """Search a child with the corresponding devpath=`path` and returns its name"""
        fp = self.get_fullpath().replace('//', '/')
        if not path.startswith(fp):
            print 'searchPath: not corresponding!', path, fp
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
                print 'searchPath: not found', repr(p), repr(path)
        return self.separator.join(vpath)

    def listPresets(self):
        return ['default']

    def iterprint(self, base=False, pre=''):
        """Iterative printing for debug purposes"""
        if not base:
            base = self
        for k, v in base.iteritems():
            print pre, k, v['current']
        for sub in base.children.iterkeys():
            base.child(sub).iterprint(pre=pre + '\t%s: ' % sub)

    @property
    def samples(self):
        if not self.has_key('nSamples'):
            return []
        n = self['nSamples']
        out = []
        for i in range(n):
            s = 'smp{}'.format(i)
            if not self.has_key(s):
                print 'sample not found', s
                break
            # Get fullpath of the referred sample object
            s = self[s][0]
            # Get the actual object
            obj = self.root.toPath(s)
            if obj is None:
                print 'sample object not found', s
                continue
            out.append(obj)
        print 'returning samples', out
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
