# -*- coding: utf-8 -*-
"""Indexing hdf5 files"""
from __future__ import unicode_literals
# NOTICE: THIS FILE IS ALSO PART OF THE CLIENT. IT SHOULD NOT CONTAIN
# REFERENCES TO THE SERVER OR TWISTED PKG.
ext = '.h5'

import os
try:
    from cPickle import dumps
except:
    from pickle import dumps
from traceback import format_exc
import functools
from tables.nodes import filenode
from tables.file import _open_files
from traceback import print_exc
from time import time
from multiprocessing import Lock

from .. import csutil
from ..csutil import lockme,  unlockme, enc_options
from ..logger import get_module_logging


def addHeader(func):
    @functools.wraps(func)
    def addHeader_wrapper(self, *a, **kw):
        if not self.test:
            return False
        rc = kw.get('reference_class', False)
        if rc is not False:
            del kw['reference_class']
        g = func(self, *a, **kw)
        if rc:
            if rc not in self._header:
                self._header[rc] = []
            self._header[rc].append(g._v_pathname)
#		self.test.flush()
        return True
    return addHeader_wrapper


class CoreFile(object):

    """Low-level HDF access functions"""

    def __init__(self, path=False, uid='', mode='a', title='', 
                 log=get_module_logging(__name__), 
                 header=True, version= '', load_conf=False):
        self._header = {}  # static header listing
        self.node_cache = {}
        self.path = False
        self.uid = False
        self._test = False  # currently opened HDF file
        self.log = log
        self._lock = Lock()
        self.version = None
        # FIXME: open_file is defined in SharedFile...!!!!
        if path is not False:
            self.open_file(path, uid, mode=mode, 
                           header=header, 
                           version=version, 
                           load_conf=load_conf)

    def fileno(self):
        return self.test.fileno()

    def get_uid(self):
        return self.uid

    def get_path(self):
        return self.path

    def get_id(self):
        r = os.path.basename(self.path)
        # Exclude the extension
        r = r.split('.')
        r = '.'.join(r[:-1])
        return r

    @property
    def test(self):
        if self._test:
            if self._test.isopen:
                return self._test
        return False

    @test.setter
    def test(self, test):
        self._test = test

    def _get_node(self, path, subpath=False):
        #		return self.test.get_node(path) # no cache
        if subpath:
            if not path.endswith('/'):
                path += '/'
            path = path + subpath
        n = self.node_cache.get(path, False)
        if n is not False:
            if not n._v_isopen:
                self.log.debug('Found closed node. Reopening:', path, n._v_isopen)
                n = False
        if n is False:
            n = self.test.get_node(path)
            # Never cache hard links
            kid = getattr(n.attrs, 'kid', False)
            if not kid or kid==path:
                self.node_cache[path] = n
        return n

    @lockme()
    def __len__(self, path=False):
        t = self.test
        if t is False:
            self.log.warning('Asking length without file')
            return 0
        # Eq. to nonzero
        if not path:
            return True
        n = self._get_node(path)
        r = len(n)
#		n.close()
        return r

    def isopen(self):
        if self.test is False:
            return False
        return self.test.isopen

    def __nonzero__(self):
        return self.test is not False

    @lockme()
    def close(self):
        self.log.debug('CoreFile.close', self.path, type(self.test))
        self.node_cache = {}
        try:
            if self.test is not False:
                self.test.close()
            for h in list(_open_files.get_handlers_by_name(self.path)):
                self.log.debug('Closing handler:', id(h), h.mode, self.path)
                h.close()
            self.log.debug('Remaining handlers:', _open_files.get_handlers_by_name(self.path))
            return True
        except:
            self.log.debug("Reopening:", format_exc())
            return False

    @lockme()
    def remove(self):
        """Delete the file from the filesystem"""
        self.close()
        os.remove(self.path)
        return True

    def reopen(self, mode=None):
        self.log.debug('Reopening', self.path)
        if not self.path:
            return False
        kw = {}
        if self.test:
            try:
                kw['mode'] = self.test.mode
                if mode and mode==kw.get('mode', None):
                    return True
                self.log.debug('Closing for reopening:', self.path)
                self.close()

            except:
                self.log.debug('While reopening', self.path)
                print_exc()
        if mode:
            kw['mode'] = mode
        self.open_file(self.path, **kw)
        return True

    ######################
    # Manipulation
    def _has_node(self, where, name=False):
        if self.test is False:
            return False
        if name:
            where += '/' + name
        if not where.startswith('/'):
            where = '/'+where
        return where in self.test

    @lockme()
    def has_node(self, where, name=False):
        return self._has_node(where, name)


    @lockme()
    def has_node_attr(self, path, attr):
        if not path.startswith('/'):
            self.log.debug('has_node_path, wrong path', path, attr)
            path = '/' + path
        n = self._get_node(path)
        r = hasattr(n.attrs, attr)
#		n.close()
        return r

    @lockme()
    def get_node_attr(self, *a, **kw):
        """Return the attribute named `attrname` of node `where`"""
        r = self.test.get_node_attr(*a, **kw)
        return csutil.xmlrpcSanitize(r)

    @lockme()
    def set_node_attr(self, *a, **kw):
        return self.test.set_node_attr(*a, **kw)

    @lockme()
    def get_attributes(self, where, name=None):
        r = {}
        n = self._get_node(where, name)
        a = n.attrs
        for key in a._v_attrnamesuser:
            r[key] = getattr(a, key)
#		n.close()
        return r

    def _set_attributes(self, where, name=None, attrs={}):
        """Non-locking call to set_node_attr on `where` with a dict of `attrs`.
        Optionally accepts leaf `name`."""
        for k, v in attrs.items():
            self.log.debug('setting node attr', where, name, k, repr(v))
            self.test.set_node_attr(where, k, v, name=name)

    @lockme()
    def set_attributes(self, *a, **kw):
        """Locking call to _set_attributes"""
        return self._set_attributes(*a, **kw)

    @lockme()
    def len(self, where):
        n = self._get_node(where)
        r = int(n.nrows)
        return r

    @lockme()
    def append_to_node(self, where, data):
        """Append data to node located in `where`"""
        if self.test is False:
            self.log.warning('Append error: Node not found', where)
            return False
        r = False
        try:
            n = self._get_node(where)
            r = n.append(data)
#			n.close()
        except:
            self.log.error('Exception appending to', where, type(data), data, repr(data))
            print_exc()
        return r

    @lockme()
    def list_nodes(self, *a, **k):
        """Return a list of node names"""
        lst = self.test.list_nodes(*a, **k)
        r = [n._v_name for n in lst]
        return r

    @lockme()
    def group_len(self, path, classname=''):
        """Returns length of objects contained in group path"""
        return len(self.test.list_nodes(path, classname=classname))

    def get_unique_name(self, where, prefix='', suffix=''):
        """Return unique name for object under `where` group with `prefix`"""
        idx = self.group_len(where)
        name = prefix + suffix  # no idx contained
        if not len(name):
            name = '0'
        while self.has_node(where, name):
            name = prefix + str(idx) + suffix
            idx += 1
        return name

    def _file_node(self, path):
        """Unlocked version of file_node"""
        if self.test is False:
            self.log.warning('CoreFile.file_node: no test', self.path)
            return ''
        node = self._get_node(path)
        self.log.debug('open file node', path)
        node = filenode.open_node(node, 'r')
        r = node.read()
# 		node.close()
        return r

    @lockme()
    def file_node(self, path):
        """Returns content of filenode in path"""
        return self._file_node(path)

    def xmlrpc_file_node(self, path):
        return csutil.binfunc(self.file_node(path))

    @lockme()
    def flush(self):
        if not self.test:
            return False
        return self.test.flush()

    ######################
    # Creation

    @lockme()
    def create_group(self, *a, **kw):
        g = self.test.create_group(*a, **kw)
        return True

    @lockme()
    @addHeader
    def create_vlarray(self, *a, **kw):
        g = self.test.create_vlarray(*a, **kw)
        return g

    @lockme()
    @addHeader
    def create_earray(self, *a, **kw):
        g = self.test.create_earray(*a, **kw)
        return g

    @lockme()
    @addHeader
    def create_table(self, *a, **kw):
        g = self.test.create_table(*a, **kw)
        return g

    @lockme()
    @addHeader
    def create_hard_link(self, *a, **k):
        g = self.test.create_hard_link(*a, **k)
        return g

    @lockme()
    def remove_node(self, path, recursive=1):
        if not self._has_node(path):
            return False

        self.test.remove_node(path, recursive=recursive)
        # Clean the cached header
        path += '/'
        for k, v in self._header.items():
            if path not in v:
                continue
            v.remove(path)
            self._header[k] = v
            break
#		self.test.flush()
        return True

    @unlockme
    def filenode_write(self, path, data='', obj=None, mode='w'):
        # TODO: better use of the mode param
        if self.test is False:
            return False
        self.reopen(mode='a')
        n = False
        attrs = {}
        self.log.debug('CoreFile.filenode_write', path)
        if self.has_node(path) and mode == 'w':
            self.log.debug('removing old node', path)
            t0 = time()
            attrs = self.get_attributes(path)
            self.log.debug('saved attributes', attrs)
            self.remove_node(path)
        self.log.debug('filenode_write lock')
        self._lock.acquire()
        where = os.path.dirname(path)
        name = os.path.basename(path)
        self.log.debug('newNode', path, where, name)
        try:
            node = filenode.new_node(self.test, where=where, name=name)
        except:
            self._lock.release()
            print_exc()
            return False
        self.log.debug('newNode done', where, name)
        if obj:
            t0 = time()
            data = dumps(obj)
            t1 = time()
            self.log.debug('dumping', t1 - t0)
            node.write(data)
            t2 = time()
            self.log.debug('writing', t2 - t1)
            self.log.debug('total', t2 - t0)
        else:
            node.write(bytes(data, **enc_options))
#		node.close()
        # Restore attributes
        self.test.flush()
        self._lock.release()
        if len(attrs) > 0:
            self.log.debug('restoring attrs', attrs)
            self.set_attributes(path, attrs=attrs)

        self.log.debug('DONE CoreFile.filenode_write', path)
        return len(data)



    def link(self, link_path, referred_path):
        """Create a new link from link_path to existing object referred_path"""
        if not self.has_node(referred_path):
            self.log.debug('Impossible to create link:', link_path, referred_path)
            return False
        v = link_path.split('/')
        name = v.pop(-1)
        where = '/'.join(v)
        if not len(where):
            where = '/'
        self.log.debug('Creating link', link_path, referred_path, where, name)
        g = self.create_hard_link(
            str(where), str(name), referred_path, createparents=True)
        if g:
            return True
        return False

    def debug(self):
        msg = ['Debug info for object', str(id(self)), str(id(self.test)),
               repr(self),
               repr(self.test)]
        msg = '\n'.join(msg)
        self.log.debug(msg)
        self.log.debug(self.test)
        return msg

    def _versioned(self, path, version=False):
        """Translate standard orig path into configured version path.
        Eg: /conf to /ver_1/conf"""
        if version is False:
            version = self.version or ''
        if version and not path.startswith(version):
            path1 = version + path
            if self._has_node(path1):
                return path1
        return path

    @lockme()
    def versioned(self, path, version=False):
        return self._versioned(path, version=version)
