# -*- coding: utf-8 -*-
"""Indexing hdf5 files"""
# NOTICE: THIS FILE IS ALSO PART OF THE CLIENT. IT SHOULD NOT CONTAIN
# REFERENCES TO THE SERVER OR TWISTED PKG.
ext = '.h5'

import os
from cPickle import dumps, loads
from traceback import format_exc
import functools
import tables
from tables.nodes import filenode
from .. import csutil
from traceback import print_exc
from time import time
from multiprocessing import Lock
from ..csutil import lockme,  unlockme


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
            if not self._header.has_key(rc):
                self._header[rc] = []
            self._header[rc].append(g._v_pathname)
#		self.test.flush()
        return True
    return addHeader_wrapper


class CoreFile(object):

    """Low-level HDF access functions"""

    def __init__(self, path=False, uid='', mode='a', title='', log=csutil.fakelogger, header=True):
        self._header = {}  # static header listing
        self.node_cache = {}
        self.path = False
        self.uid = False
        self._test = False  # currently opened HDF file
        self.log = log
        self._lock = Lock()
        # FIXME: open_file is defined in SharedFile...!!!!
        if path is not False:
            self.open_file(path, uid, mode=mode, header=header)

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
                print 'Found closed node. Reopening:', path, n._v_isopen
                n = False
        if n is False:
            n = self.test.get_node(path)
            self.node_cache[path] = n
        return n

    @lockme
    def __len__(self, path):
        t = self.test
        if t is False:
            print 'Asking length without file'
            return 0
        n = self._get_node(path)
        r = len(n)
#		n.close()
        return r

    def isopen(self):
        if self.test is False:
            return False
        return self.test.isopen

    def __nonzero__(self):
        return self.test != False

    @lockme
    def close(self):
        print 'CoreFile.close', self.path, type(self.test)
        self.node_cache = {}
        try:
            if self.test is not False:
                # 				print 'closing',self.path
                self.test.close()
            return True
        except:
            # 			if tables.file._open_files.has_key(self.path):
            # 				del tables.file._open_files[self.path]
            self.log.debug("Reopening:", format_exc())
            return False

    @lockme
    def remove(self):
        """Delete the file from the filesystem"""
        self.close()
        os.remove(self.path)
        return True

    def reopen(self):
        print 'Reopening', self.path
        if self.test:
            try:
                self.test.close()
            except:
                print 'While reopening', self.path
                print_exc()
        self.open_file(self.path)
        return True

    ######################
    # Manipulation
    @lockme
    def has_node(self, where, name=False):
        if self.test is False:
            return False
        if name:
            where += '/' + name
        return where in self.test

    @lockme
    def has_node_attr(self, path, attr):
        if not path.startswith('/'):
            print 'has_node_path, wrong path', path, attr
            path = '/' + path
        n = self._get_node(path)
        r = hasattr(n.attrs, attr)
#		n.close()
        return r

    @lockme
    def get_node_attr(self, *a, **kw):
        """Return the attribute named `attrname` of node `where`"""
        r = self.test.get_node_attr(*a, **kw)
        return csutil.xmlrpcSanitize(r)

    @lockme
    def set_node_attr(self, *a, **kw):
        return self.test.set_node_attr(*a, **kw)

    @lockme
    def get_attributes(self, where, name=None):
        print 'getting attributes from', where
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
        for k, v in attrs.iteritems():
            print 'setting node attr', where, name, k, v
            self.test.set_node_attr(where, k, v, name=name)

    @lockme
    def set_attributes(self, *a, **kw):
        """Locking call to _set_attributes"""
        return self._set_attributes(*a, **kw)

    @lockme
    def len(self, where):
        # 		print 'len',where
        n = self._get_node(where)
# 		print 'asking length',where,n,n.nrows
        r = int(n.nrows)
#		n.close()
        return r

    @lockme
    def append_to_node(self, where, data):
        """Append data to node located in `where`"""
        if self.test is False:
            print 'Append error: Node not found', where
            return False
        r = False
        try:
            n = self._get_node(where)
            r = n.append(data)
#			n.close()
        except:
            print 'Exception appending to', where, type(data), data, repr(data)
            print_exc()
        return r

    @lockme
    def list_nodes(self, *a, **k):
        """Return a list of node names"""
        lst = self.test.list_nodes(*a, **k)
        r = [n._v_name for n in lst]
        return r

    @lockme
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
            print 'CoreFile.file_node: no test', self.path
            return ''
        node = self._get_node(path)
        print 'open node', path
        node = filenode.openNode(node, 'r')
        r = node.read()
# 		node.close()
        return r

    @lockme
    def file_node(self, path):
        """Returns content of filenode in path"""
        return self._file_node(path)

    def xmlrpc_file_node(self, path):
        return csutil.binfunc(self.file_node(path))

    @lockme
    def flush(self):
        if not self.test:
            return False
        return self.test.flush()

    ######################
    # Creation

    @lockme
    def create_group(self, *a, **kw):
        g = self.test.createGroup(*a, **kw)
        return True

    @lockme
    @addHeader
    def create_vlarray(self, *a, **kw):
        g = self.test.createVLArray(*a, **kw)
        return g

    @lockme
    @addHeader
    def create_earray(self, *a, **kw):
        g = self.test.createEArray(*a, **kw)
        return g

    @lockme
    @addHeader
    def create_table(self, *a, **kw):
        g = self.test.createTable(*a, **kw)
        return g

    @lockme
    @addHeader
    def create_hard_link(self, *a, **k):
        g = self.test.create_hard_link(*a, **k)
        return g

    @lockme
    def remove_node(self, path, recursive=1):
        self.test.removeNode(path, recursive=recursive)
        # Clean the cached header
        path += '/'
        for k, v in self._header.iteritems():
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
        n = False
        attrs = {}
        print 'SharedFile.filenode_write', path
        if self.has_node(path) and mode == 'w':
            print 'removing old node', path
            t0 = time()
            attrs = self.get_attributes(path)
            print 'saved attributes', attrs
            self.remove_node(path)
        print 'filenode_write lock'
        self._lock.acquire()
        where = os.path.dirname(path)
        name = os.path.basename(path)
        print 'newNode', path, where, name
        try:
            node = filenode.newNode(self.test, where=where, name=name)
        except:
            self._lock.release()
            print_exc()
            return False
        print 'newNode done', where, name
        if obj:
            t0 = time()
            data = dumps(obj)
            t1 = time()
            print 'dumping', t1 - t0
            node.write(data)
            t2 = time()
            print 'writing', t2 - t1
            print 'total', t2 - t0
        else:
            node.write(data)
#		node.close()
        # Restore attributes
        self.test.flush()
        self._lock.release()
        if len(attrs) > 0:
            print 'restoring attrs', attrs
            self.set_attributes(path, attrs=attrs)

        print 'DONE SharedFile.filenode_write', path
        return len(data)

    def debug(self):
        msg = ['Debug info for object', str(id(self)), str(id(self.test)),
               repr(self),
               repr(self.test)]
        msg = '\n'.join(msg)
        print msg
        print self.test
        return msg
