# -*- coding: utf-8 -*-
"""Option persistence."""
import re
import collections
import numpy as np

from .. import logger
from conf import Conf
import cPickle as pickle
from ..milang import Scriptable
import common_proxy

logging = logger.get_module_logging(__name__)


def dictRecursiveModel(base):
    """BUild a dictionary configuration tree from ConfigurationProxy `base`"""
    out = collections.OrderedDict()
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


# TODO: parametrize in user conf
# 0=always visible; 1=user ; 2=expert ; 3=advanced ; 4=technician ;
# 5=developer; 6=never visible
class ConfigurationProxy(Scriptable, Conf):

    """A configuration object behaving like a live server"""
    separator = '/'
    _readLevel = 5
    _writeLevel = 5
    _rmodel = False
    callbacks_get = {}
    callbacks_set = {}
    doc = False # Document where this configuration is contained
    filename = False # Filename from which this configuration was red

    def print_tree(self):
        print print_tree(self.tree())

    def __init__(self, desc=collections.OrderedDict({'self': {}}),
                 name='MAINSERVER', parent=False, readLevel=-1, writeLevel=-1, kid_base='/'):
        Scriptable.__init__(self)
        self.log = logger.BaseLogger()
        self.kid_base = kid_base
        Conf.__init__(self, desc['self'])
        if readLevel > 0:
            self._readLevel = readLevel
        if writeLevel > 0:
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
#           if parent._parent:
#               self._Method__name=parent._Method__name+self.separator+name
#           else:
#               self._Method__name=name
        if self.has_key('devpath'):
            self['devpath'] = name
        self.autosort()

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
        r = '/' + '/'.join(path) + '/'
        self['fullpath'] = r
        return r

    def _update_from_children(self):
        """ Retrieve current configuration from instantiated child objects"""
        for key, obj in self.children_obj.iteritems():
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
        return self.desc.iteritems()

    def itervalues(self):
        return self.desc.itervalues()

    def values(self):
        return self.desc.values()

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
        self.doc = obj.doc 
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
        if len(a) == 1 and not self.desc.has_key(key):
            return a[0]
        return self.desc[key]['current']

    def callback(self, key, val, callback_name='set'):
        callback_group = getattr(self, 'callbacks_' + callback_name)
        cb = callback_group.get(
            self.desc[key].get('callback_' + callback_name, False), False)
        if cb:
            old = self.desc[key]['current']
            val = cb(self, key, old, val)
            self.log.debug('Callback:', key, old, val)
        return val

    def __setitem__(self, key, val):
        if not self.desc.has_key(key):
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

    def sete(self, key, val):
        if not val.has_key('priority') or val['priority'] == -1:
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
        def sorter(item):
            key, val = item
            digits = re.sub(r"\D", '', key)
            key = int(digits) if len(digits) else key
            return key
        self.children = collections.OrderedDict(
            sorted(self.children.items(), key=sorter))

    def add_child(self, name, desc, overwrite=False):
        """Inserts a sub-object `name` with object tree dictionary `desc`.
        Returns a ConfigurationProxy to the new child."""
        # If desc is the description dictionary and not the object tree dictionary,
        # encapsulate it in a 'self' dict
        if not 'self' in desc:
            desc = {'self': desc}
        if overwrite or name not in self.children:
            self.children[name] = desc
        else:
            for key, val in desc['self'].iteritems():
                self.children[name]['self'][key] = val
        self.autosort()
        r = self.child(name)
        r.get_fullpath()
        return r

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
            obj = ConfigurationProxy(self.children[name],
                                                         name=name, parent=self, kid_base=kb,
                                                         readLevel=self._readLevel, writeLevel=self._writeLevel)
            obj.doc = self.doc
            obj.filename = self.filename
            self.children_obj[name] = obj
        return self.children_obj[name]

    def calc_aggregate(self, aggregation, handle=False):
        # TODO: move to Scriptable class! (or a new one)
        function_name = re.search("(.+?)\(", aggregation).group(1)
        targets = re.search("\((.+?)\)", aggregation)
        if targets is None:
            targets = [handle]
        else:
            targets = targets.group(1).replace(' ', '').split(',')
        values = collections.defaultdict(list)
        for child_name in self.children.iterkeys():
            child = self.child(child_name)
            pack = collections.defaultdict(list)
            for target in targets:
                # Ensure all targets exist
                if not child.has_key(target):
                    pack = False
                    break
                pack[target].append(child[target])
            if pack:
                for t in targets:
                    values[t] += pack[t]
        result = None
        error = None
        # TODO: calc stdev here
        if function_name == 'mean':
            v = np.array(values[targets[0]]).astype(np.float32)
            # FIXME: hack to filter out zeros
            v = v[v != 0]
            if len(v):
                result = float(v.mean())
                error = float(v.std())
        elif function_name == 'sum':
            result = float(
                np.array(values[targets[0]]).astype(np.float32).sum())
        elif function_name == 'prod':
            result = float(
                np.array(values[targets[0]]).astype(np.float32).prod())
        elif function_name == 'table':
            result = []
            for i, x in enumerate(values[targets[0]]):
                row = []
                for t in targets:
                    row.append(values[t][i])
                result.append(row)
            # Reorder by first column
            result = sorted(result, key=lambda e: e[0])
            # Prepend table header
            # Should calculate also table header?
            result = [self[handle][0]] + result
        else:
            self.log.error(
                'Aggregate function not found:', function_name, aggregation)
        return result, error

    def update_aggregates(self, recursive=1):
        """Updates aggregate options. recursive==1, upward; -1, downward"""
        # TODO: move to Scriptable class! (or a new one)
        for handle, opt in self.desc.iteritems():
            if not opt.has_key('aggregate'):
                continue
            aggregation = opt['aggregate']
            result, error = self.calc_aggregate(aggregation, handle)
            if result is not None:
                self[handle] = result
                if error is not None and opt.has_key('error'):
                    self[opt['error']] = error
            else:
                self.log.error('Aggregation failed for ', handle, aggregation)
        if recursive > 0 and self.parent():
            self.parent().update_aggregates(recursive=1)
        elif recursive < 0:
            for k in self.children.keys():
                self.child(k).update_aggregates(recursive=-1)
            # Then run backwards as aggregates only propagates bottom-up
            if self.parent():
                self.parent().update_aggregates(recursive=1)

    @property
    def devices(self):
        return [self.child(name) for name in self.children.keys()]

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
        if not self.measure.has_key('nSamples'):
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
            if not self.has_key(s):
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
