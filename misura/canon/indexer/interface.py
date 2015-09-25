# -*- coding: utf-8 -*-
"""Indexing hdf5 files"""
# NOTICE: THIS FILE IS ALSO PART OF THE CLIENT. IT SHOULD NOT CONTAIN
# REFERENCES TO THE SERVER OR TWISTED PKG.
ext = '.h5'

import os
from cPickle import dumps, loads
from traceback import format_exc
import tables
from .. import csutil
from datetime import datetime
from .. import option
import numpy as np
from ..csutil import lockme
from .. import reference

from corefile import CoreFile
from dataops import DataOperator
import digisign
from digisign import list_references

max_string_length = 1000

# To disable @lockme locking:
# lockme=lambda func: func
tables.file._FILE_OPEN_POLICY = 'default'


def pathnode(path):
    """Split a complete path into its group and leaf components"""
    while path.endswith('/'):
        path = path[:-1]
    path = path.split('/')
    node = path.pop(-1)
    return '/'.join(path), node


class SharedFile(CoreFile, DataOperator):

    """Interface for test file access. Versioning.
    TODO: move xmlrpc to server-side OutFile?"""

    start_profiler = csutil.start_profiler
    stop_profiler = csutil.stop_profiler
    version = ''
    """Current file version"""
    conf = False
    """Configuration dictionary"""

    def __init__(self, *a, **k):
        CoreFile.__init__(self, *a, **k)
        self.conf = False
        self.node_cache = {}

    def open_file(self, path=False, uid='', mode='a', title='', header=True):
        """opens the hdf file in `path` or `uid`"""
        self.node_cache = {}
        if not path:
            path = self.path
        if not path:
            self.log.debug('No path supplied', path, self.path, uid)
            return False
        if mode == 'w':
            self.log.debug('Creating in write mode', path)
            tables.openFile(path, mode='w', title=title).close()
        if not os.path.exists(path):
            raise RuntimeError("File %s not found." % path)

        try:
            print 'opening existing file', path, mode
            self.test = tables.openFile(path, mode=mode)
            self.path = path
        except:
            self.log.error('Error opening file:', format_exc(), path)
            return False
        self.uid = uid
        if header and mode != 'w':
            if self.has_node('/conf'):
                if self.has_node_attr('/conf', 'uid'):
                    self.uid = self.get_node_attr('/conf', 'uid')
                self.load_conf()
            else:
                self.conf = option.ConfigurationProxy()
            self.header(refresh=True)
        print 'done open_file', self.path
        return self.test, self.path

    def load_conf(self):
        d = self.conf_tree()
        self.conf = option.ConfigurationProxy(desc=d)
        print 'load conf', self.conf, len(d)
        return True

    def verify(self):
        if not self.test:
            return False
        return digisign.verify(self.test)

    def get_versions(self):
        """List available versions. Returns a dictionary {path: (name,date)}"""
        if not self.test:
            return {}
        if not self.has_node('/conf'):
            return {}
        m = getattr(self.test.root.conf.attrs, 'versions', 10)
        v = {'': ('Original', self.test.root.conf.attrs.date)}
        # skip 0 and seek a little farer
        latest = 0
        for i in range(1, m + 5):
            n = '/ver_{}'.format(i)
            if n in self.test:
                name = self.get_node_attr(n, 'name')
                date = self.get_node_attr(n, 'date')
                v[n] = (name, date)
                latest = i
        self.test.root.conf.attrs.versions = latest
        print 'returning versions', v
        return v

    def get_version(self):
        return self.version

    def set_version(self, newversion=-1):
        """Set the current version to `newversion`"""
        print 'setting version to ', newversion
        # Find latest version
        if newversion < 0:
            self._lock.acquire()
            newversion = getattr(self.test.root.conf.attrs, 'versions', 0)
            self._lock.release()
        if newversion == 0 or newversion == '':
            print 'set_version original'
            if self.version != '':
                self.version = ''
                self.load_conf()
            self.version = ''
            return True
        # Convert to path
        if isinstance(newversion, int):
            newversion = '/ver_{}'.format(newversion)
        if self.conf and self.version != newversion:
            # Reload conf
            print 'version changed: reloading', newversion, self.version
            self.version = newversion
            self.load_conf()
            return True

        self.version = newversion
        return True

    @lockme
    def create_version(self, name=False):
        """Create a new version with `name`"""
        latest = getattr(self.test.root.conf.attrs, 'versions', 0)
        newversion = '/ver_{}'.format(latest + 1)
        print 'creating new version', newversion, name
        if not name:
            name = newversion
        self.test.create_group('/', newversion[1:])
        d = datetime.now().strftime("%H:%M:%S, %d/%m/%Y")
        self._set_attributes(newversion, attrs={'name': name, 'date': d})
        self.test.root.conf.attrs.versions = latest + 1
        # Set current version (will be empty until some transparent writing
        # occurs)
        self.version = newversion
        self.test.flush()
        return newversion

    @lockme
    def get_plots(self, render=False):
        """List available plots. Returns a dictionary {path: (name,date,render,render_format)}"""
        r = {}
        if not self.test:
            return r
        if not '/plot' in self.test:
            return r
        image = False
        # TODO: read format
        image_format = False
        for node in self.test.list_nodes('/plot'):
            path = '/plot/{}/'.format(node._v_name)
            script = self.test.get_node(path + 'script')
            if render:
                if path + 'render' in self.test:
                    image = self._file_node(path + 'render')
                else:
                    image = False
            r[node._v_name] = (
                script.attrs.title, script.attrs.date, image, image_format)
        return r

    def get_plot(self, plot_id):
        """Returns the text of a plot"""
        return self.file_node('/plot/{}/script'.format(plot_id))

    def save_plot(self, text, plot_id=False, title=False, date=False, render=False, render_format=False):
        """Save the text of a plot to plot_id, optionally adding a title, date and rendered output"""
        if not self.has_node('/plot'):
            self.create_group('/', 'plot')
            if not plot_id:
                plot_id = '0'

        if not plot_id:
            plot_id = self.get_unique_name('/plot')
        if not title:
            title = plot_id
        current_date = datetime.now().strftime("%H:%M:%S, %d/%m/%Y")
        if not date:
            date = current_date

        base_group = '/plot/' + plot_id
        if not self.has_node(base_group):
            self.create_group('/plot', plot_id)

        text_path = base_group + '/script'
        self.filenode_write(text_path, data=text)
        self.set_attributes(text_path, attrs={'title': title, 'date': date})

        if render and render_format:
            render_path = base_group + '/render'
            self.filenode_write(render_path, data=render)
            self.set_attributes(render_path, attrs={'format': render_format})

        return plot_id, title, date

    def getLog(self):
        # FIXME: show logging
        return 'unimplemented'
        txt = ''
        for line in self.test.root.log:
            txt += reference.Binary.decode(line)[1]
            if not txt.endswith('\n'):
                txt += '\n'
        return txt

    def conf_tree(self):
        tree = self.file_node(self.versioned('/conf'))
        if tree in [False, None]:
            print 'Configuration node file not found!'
            return '{}'
        # test
        print 'loading ', len(tree)
        d = loads(tree)
        if type(d) != type({}):
            print 'Wrong Conf Tree!'
            return False
        print 'Conf tree length:', len(tree)
        return d

    def xmlrpc_conf_tree(self):
        t = self.file_node(self.versioned('/conf'))
        if t is False:
            return t
        return csutil.binfunc(t)

    def save_conf(self, tree=False, writeLevel=3):
        """Saves a new version of the configuration tree"""
        if not tree:
            if not self.conf:
                self.load_conf()
            tree = self.conf.tree()
        self.filenode_write(self.version + '/conf', obj=tree)
        if self.version != '':
            a = self.get_attributes('/conf')
            self.set_attributes(self.version + '/conf', attrs=a)
        return

    @lockme
    def header(self, reference_classes=['Array'], startswith=False, refresh=False):
        """Returns all available data references"""
        if refresh or len(self._header) == 0:
            self._header = list_references(self.test.root)
            print self._header
        if reference_classes is False:
            reference_classes = self._header.keys()
        r = []
        for k in reference_classes:
            r += self._header.get(k, [])
        if startswith:
            filtered = []
            for e in r:
                if e.startswith(startswith):
                    filtered.append(e)
            return filtered
        return r

    def xmlrpc_col(self, *a, **k):
        r = self.col(*a, **k)
        return csutil.binfunc(dumps(r))

    def xmlrpc_col_at(self, *a, **k):
        r = self.col_at(*a, **k)
        return csutil.binfunc(dumps(r))

    @lockme
    def get_decoded(self, path, idx, get):
        """Get the `path` node index `idx` using the getter function `get`"""
        n = self._get_node(path)
        r = get(n, idx)
#		n.close()
        return r

    @lockme
    def query_time(self, path, startTime=-1,  endTime=-1, step=None, interp=False):
        """Reads an array in the requested time range"""
        n = self._get_node(path)
        # TODO: adapt also to other Reference objects
        t = n.cols.t
        if startTime < 0:
            startTime = t[0]
        if endTime <= 0:
            endTime = t[-1]
        if startTime > endTime:
            self.log.error('impossible time frame', startTime, endTime)
#			n.close()
            return []
        si = csutil.find_nearest_val(t, startTime)
        ei = csutil.find_nearest_val(t, endTime)
        print startTime, si, endTime, ei
        arr = n[si:ei]
#		n.close()
        if step is None:
            return arr
        # Interpolate for time stepping
        st = t[si]
        et = t[ei]
        ts = np.arange(st, et, step)
        print 'tseq', st, et, step, ts
        if interp:
            r = self.interpolated_col(
                arr=arr, startIdx=0, endIdx=-1, time_sequence=ts)
            r = np.array([ts, r])
            r = r.transpose()
        else:
            r = arr
        return r

    def xmlrpc_query_time(self, *a, **k):
        r = self.query_time(*a, **k)
        return csutil.binfunc(dumps(r))

    def xmlrpc_interpolated_col(self, *a, **k):
        r = self.interpolated_col(*a, **k)
        return csutil.binfunc(dumps(r))

    def instrument_name(self):
        return self.get_node_attr('/conf', 'instrument')

    def run_scripts(self, instr=None):
        """Re-evaluate scripts"""
        if instr is None:
            instr = getattr(self.conf, self.instrument_name, None)
        if instr is None:
            print 'Impossible to run scripts: conf is not available.'
            return False
        if self.conf.kiln is not None:
            instr.kiln = self.conf.kiln
        # Associate scripts to their output Meta options
        instr.outFile = self
        instr.distribute_scripts(self)
        instr.characterization(period = 'all')
        return True

    def copy(self):
        return self

    def connect(self):
        return True

    def has_key(self, *a, **k):
        return False

    def decode(self, method):
        """Return if a method name should be decoded client-side"""
        return hasattr(self, 'xmlrpc_' + method)
