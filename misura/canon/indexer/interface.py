# -*- coding: utf-8 -*-
"""Indexing hdf5 files"""
from __future__ import unicode_literals
# NOTICE: THIS FILE IS ALSO PART OF THE CLIENT. IT SHOULD NOT CONTAIN
# REFERENCES TO THE SERVER OR TWISTED PKG.
ext = '.h5'

import os
try:
    from cPickle import dumps, loads
except:
    from pickle import dumps, loads
    unicode = str
    basestring = str
from traceback import format_exc
import tables
from datetime import datetime
import numpy as np
from ..parameters import cfilter
from .. import csutil
from .. import option
from ..csutil import lockme, enc_options, str3
from .. import reference

from .corefile import CoreFile
from .dataops import DataOperator
from . import digisign
from .digisign import list_references

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
    version = None
    """Current file version"""
    conf = False
    """Configuration dictionary"""

    def __init__(self, *a, **k):
        self.conf = False
        CoreFile.__init__(self, *a, **k)
        self.node_cache = {}

    def open_file(self, path=False, uid='', mode='a', title='', header=True, version='', load_conf=True):
        """opens the hdf file in `path` or `uid`"""
        self.node_cache = {}
        if not path:
            path = self.path
        if not path:
            self.log.debug('No path supplied', path, self.path, uid)
            return False
        # Always open the real, normalize path
        path = os.path.realpath(os.path.normpath(path))

        if mode == 'w':
            self.log.debug('Creating in write mode', path)
            tables.open_file(path, mode='w', title=title).close()
        if not os.path.exists(path):
            raise RuntimeError("File %s not found." % path)

        try:
            self.log.debug('opening existing file', path, mode, repr(version))
            if mode=='r' and self.highest_mode(path)=='a':
                self.close_handlers(path)
            self.test = tables.open_file(path, mode=mode)
            self.path = path
        except:
            self.log.error('Error opening file:', format_exc(), path)
            return False
        self.uid = uid
        if header and mode != 'w':
            if not self.has_node('/userdata') and mode != 'r':
                self.create_group('/', 'userdata')
                self.set_attributes('/userdata', attrs={'active_version': ''})
            elif not version:
                version = self.active_version()
                if not self.has_node(version):
                    self.log.info('VERSION DOES NOT EXIST', version)
                    version = ''
            if self.has_node('/conf'):
                if self.has_node_attr('/conf', 'uid'):
                    self.uid = self.get_node_attr('/conf', 'uid')
                if version != None:
                    self.set_version(version, load_conf=load_conf)
            else:
                self.log.info(
                    'No configuration object was found', path, version)
                self.header(refresh=False, version=self.get_version())
        if self.conf is False:
            self.conf = option.ConfigurationProxy()
        return self.test, self.path

    def writable(self):
        if not self.test:
            return False
        return self.test.mode in ('a', 'r+')

    def load_conf(self):
        d = self.conf_tree()
        self.conf = option.ConfigurationProxy(desc=d)
        self.conf.filename = self.path
        self.log.debug('load conf', self.conf, len(d))
        return True

    def verify(self):
        if not self.test:
            return False
        return digisign.verify(self.test)

    def get_versions(self):
        """List available versions. Returns a dictionary {path: (name,date)}"""
        if not self.test:
            self.log.debug('get_versions: no test defined')
            return {}
        if not self._has_node('/conf'):
            self.log.debug('get_versions: no /conf')
            return {}
        v = {'': ('Original', self.test.root.conf.attrs.date)}
        # skip 0 and seek a little farer
        latest = 0
        for node in self.test.list_nodes('/'):
            name = str(node._v_name)
            if not name.startswith('ver_'):
                continue
            ver = int(name.split('_')[-1])
            if ver > latest:
                latest = ver
            v[str(node._v_pathname)] = (node._f_getattr('name'),
                                        node._f_getattr('date'))
        if self.writable():
            self.test.root.conf.attrs.versions = latest
        self.log.debug('returning versions', v)
        return v

    def get_version_by_name(self, name):
        for path, data in self.get_versions().items():
            if data[0] != name:
                continue
            return path
        return False

    def get_latest_version_number(self):
        v = [0]
        for k in self.get_versions().keys():
            v.append(0 if k == '' else int(k.split('_')[-1]))
        m = max(v)
        return m

    def get_versions_by_date(self):
        v = self.get_versions()
        v = [[key] + list(val) for key, val in v.items()]
        v.sort(key=lambda e: datetime.strptime(e[2]))

    def get_version(self):
        return self.version or ''

    def set_version(self, newversion=-1, load_conf=True):
        """Set the current version to `newversion`"""
        # Load the last used version
        if newversion == -1:
            found = False
            self._lock.acquire()
            if '/userdata' in self.test:
                newversion = self._active_version()
                if self._has_node(newversion):
                    self.log.debug('Found active version', newversion)
                    found = True
            if not found:
                newversion = getattr(
                    self.test.root.conf.attrs, 'versions', 0) - 1
                if newversion > 0:
                    newversion = '/ver_{}'.format(newversion)
                    if self._has_node(newversion):
                        self.log.debug('Take latest version', newversion)
                        found = True
            if not found:
                newversion = ''
                self.log.debug(
                    'Last version was not found. Taking original')
            self._lock.release()

        # Load version by number (deprecated)
        if not isinstance(newversion, basestring):
            newversion = '/ver_{}'.format(newversion)

        if self.version == newversion and len(self.conf):
            self.log.debug('Not changing version!', self.version, newversion)
            return True

        self._change_version(newversion)
        if load_conf:
            self.load_conf()
        self.header(refresh=False)
        return True

    def _change_version(self, new_version):
        self.log.debug('Changing version to {}, {} (old was {})'.format(
            repr(new_version),
            type(new_version),
            self.version))
        self.version = str(new_version)
        if self.writable():
            self._set_attributes(
                '/userdata', attrs={'active_version': new_version})
            return True
        return False

    def create_version(self, name=False, overwrite=True):
        """Create a new version with `name`. `overwrite` a previous version with same name."""
        self.reopen(mode='a')
        newversion = False
        if name:
            newversion = self.get_version_by_name(name)
        if newversion and overwrite:
            self.log.debug('Found version', name, 'saved as',
                           newversion, '. Overwriting.')
            latest = int(newversion.split('_')[-1])
            #self.remove_version(newversion, remove_plots=False)
        else:
            latest = self.get_latest_version_number() + 1
            newversion = '/ver_{}'.format(latest)

        if not name:
            name = newversion
        name = unicode(name).encode('ascii', 'ignore')
        if not self.has_node('/', newversion[1:]):
            self.log.debug('creating new version', newversion, name)
            self.test.create_group('/', newversion[1:])
        else:
            self.log.debug('using existing version', newversion, name)
        d = datetime.now().strftime("%H:%M:%S, %d/%m/%Y")
        self._set_attributes(newversion, attrs={'name': name, 'date': d})
        self.test.root.conf.attrs.versions = latest
        # Set current version (will be empty until some transparent writing
        # occurs)
        self.version = str(newversion)
        self.log.debug('New version is now active', newversion)
        self._change_version(newversion)
        self.test.flush()
        return newversion

    def remove_version(self, version_path, remove_plots=True):
        self.reopen(mode='a')
        if remove_plots:
            self.remove_node(version_path, recursive=True)
            self.log.info('Removed version', version_path)
        else:
            for node in self.list_nodes():
                if node == 'plots':
                    continue
                self.remove_node(version_path + '/' + node)
        if version_path == self.version:
            self._change_version('')
        n = int(version_path.split('_')[-1]) - 1
        if self.test.root.conf.attrs.versions == n:
            self.test.root.conf.attrs.versions -= 1
        if self._active_version() == version_path:
            self.log.debug('Resetting active_version to Original')
            self.test.set_node_attr('/userdata', 'active_version', '')
        self.header(refresh=True)
        self.log.debug('Removed version', version_path, n)
        self.flush()
        return True

    @lockme()
    def get_plots(self, render=False, version=False):
        """List available plots in `version` (current if False). 
        Returns a dictionary {path: (name,date,render,render_format)}"""
        r = {}
        if not self.test:
            return r
        plots_path = self._versioned('/plot', version=version)
        if not plots_path in self.test:
            return r
        image = False
        # TODO: read format
        image_format = False
        for node in self.test.list_nodes(plots_path):
            path = plots_path + '/{}/'.format(node._v_name)
            script = self.test.get_node(path + 'script')
            image = False
            if render:
                if path + 'render' in self.test:
                    image = self._file_node(path + 'render')
                    
            r[node._v_name] = (script.attrs.title, script.attrs.date,
                               image, script.attrs.format)
        return r

    def get_plot(self, plot_id):
        """Returns the text of a plot"""
        n = self.versioned('/plot') + '/{}/script'.format(plot_id)
        text = self.file_node(n)
        attrs = self.get_attributes(n)
        return text, attrs

    def save_plot(self, text, plot_id=False,
                  title=False,
                  date=False,
                  render=False,
                  render_format=False):
        """Save the text of a plot to plot_id, optionally adding a title, date and rendered output"""
        if not self.version:
            self.log.error(
                'Cannot save plots for original version. Please make a new version first.')
            return False
        self.reopen(mode='a')
        plots_path = self.get_version() + '/plot'
        if not self.has_node(plots_path):
            self.create_group(self.versioned('/'), 'plot')
            if not plot_id:
                plot_id = '0'

        if not plot_id:
            plot_id = self.get_unique_name(plots_path)
        if not title:
            title = plot_id
        if not date:
            date = datetime.now().strftime("%H:%M:%S, %d/%m/%Y")

        base_group = plots_path + '/' + plot_id
        if not self.has_node(base_group):
            self.create_group(plots_path, plot_id)

        text_path = base_group + '/script'
        self.filenode_write(text_path, data=text)
        self.set_attributes(text_path, attrs={'title': title,
                                              'date': date,
                                              'format': render_format})

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

    def conf_tree(self, path=False):
        if not path:
            path = self.versioned('/conf')
        self.log.debug('Loading conf', path)
        tree = self.file_node(path)
        if tree in [False, None]:
            self.log.warning('Configuration node file not found!', path)
            return '{}'
        # test
        self.log.debug('loading ', len(tree))
        opt = enc_options.copy()
        if 'encoding' in opt:
            opt['encoding'] = 'latin1'
        d = loads(tree, **opt)
        if not isinstance(d, dict):
            self.log.debug('Wrong Conf Tree!')
            return False
        self.log.debug('Conf tree length:', len(tree))
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
        ver = self.get_version()
        self.filenode_write(ver + '/conf', obj=tree)
        if ver != '':
            a = self.get_attributes('/conf')
            self.set_attributes(ver + '/conf', attrs=a)
        self.conf = option.ConfigurationProxy(desc=tree)
        return

    def save_data(self, path, data, time_data, opt=False):
        version = self.active_version()
        if version is '':
            raise RuntimeError(
                "Original version is not writable.\nCreate or switch to another version first.")
        path = path.split(':')[-1]
        path = ("/summary/" + path).replace('//', '/')
        vpath = path.split("/")
        parent = "/".join(vpath[0:-1])
        name = vpath[-1]
        newparent = version + parent
        path = newparent + "/" + name
        if not opt:
            opt = self.get_attributes(parent + '/' + name)
            opt['handle'] = name

        self.remove_node(path)
        # Detect fixed time
        td = [1, 0]
        if len(time_data) > 10:
            td = np.diff(time_data)
        if max(td) - min(td) > 1e-14:
            # Regular Array
            write_data = np.transpose(np.vstack((time_data, data)))
            array_cls = reference.Array
        else:
            # FixedTimeArray
            write_data = np.transpose(data)
            array_cls = reference.FixedTimeArray
            opt['t0'] = time_data[0]
            opt['dt'] = td.mean()
        dest_path_reference = array_cls(
            self, newparent, opt=opt, with_summary=False)
        dest_path_reference.append(write_data)
        self.flush()
        if path not in self._header[array_cls.__name__]:
            self._header[array_cls.__name__].append(path)

    def active_version(self):
        try:
            return self.get_node_attr('/userdata', 'active_version')
        except:
            return ''

    def _active_version(self):
        try:
            return self.test.get_node_attr('/userdata', 'active_version')
        except:
            return ''
        
    def _write_userdata_header(self, h):
        """Save header dict into userdata cache.
        No lock."""
        from time import time
        t0 = time()
        if not self._has_node('/userdata'):
            self.create_group('/', 'userdata')
            self.set_attributes('/userdata', attrs={'active_version': ''})
        # Write each header entry in a separate variable-length-array
        for cls_name in h:
            name = 'header_'+cls_name
            if self._has_node('/userdata', name):
                self.test.remove_node('/userdata',name)
            vla = self.test.create_vlarray(where='/userdata',
                                    name=name,
                                    atom=tables.StringAtom(itemsize=1),
                                    title='Header cache for '+cls_name,
                                    filters=cfilter)
            for dsn in h[cls_name]:
                vla.append(list(dsn))
            self.log.debug('Wrote cached header class', cls_name, len(h[cls_name]))
        # put only the keys in the header attr
        self.test.set_node_attr('/userdata', 'header', list(h.keys()))
        self.log.debug('Finished writing headers cache', len(h), 1000 * (time() - t0))
                
    def _read_userdata_header(self):
        """Load header dict from userdata cache.
        No lock"""
        from time import time
        t0 = time()
        h = {}
        if not self._has_node('/userdata'):
            self.log.debug('_read_userdata_header: no /userdata')
            return h
        try:
            keys = self.test.get_node_attr('/userdata', 'header')
        except:
            self.log.error(format_exc())
            keys = []
            
        # Compatibility with old caching mechanism
        if isinstance(keys, dict):
            self.log.debug('Reading old header cache', keys)
            return keys
        
        for cls_name in keys:
            name = 'header_'+cls_name
            if not self._has_node('/userdata', name):
                self.log.error('Could not find header class', cls_name)
                continue
            n = self._get_node('/userdata', name)
            v = []
            for path in n:
                v.append(''.join(path))
            h[cls_name] = v
            self.log.debug('Loaded header class:',cls_name, len(v))
            
        self.log.debug('Loaded cached header', len(h), 1000 * (time() - t0))
        return h
        

    @lockme()
    def header(self, reference_classes=['Array'], startswith=False, refresh=False, version=False):
        """Returns all available data references"""
        from time import time
        if not version:
            version = self.get_version()

        # Try to read cached header from file
        if not refresh and len(self._header) == 0:
            self._header = self._read_userdata_header()
        
        # Rebuild the header if empty or refresh
        if refresh or len(self._header) == 0:
            t0 = time()
            self._header = list_references(self.test.root)
            self.log.debug('References', 
                           len(self._header), 
                           1000 * (time() - t0))
            if self.writable():
                self._write_userdata_header(self._header)

        if reference_classes is False:
            reference_classes = self._header.keys()
        r = []
        for k in reference_classes:
            r += self._header.get(k, [])
        if startswith:
            swv = version + startswith
            r = filter(lambda el: el.startswith(
                startswith) or el.startswith(swv), r)
        if not version:
            r = filter(lambda el: not el.startswith('/ver_'), r)
        else:
            # Exclude element with wrong version
            def good(el): return el.startswith(
                version + '/') or not el.startswith('/ver_')
            r = filter(good, r)
            # Exclude unversioned elements having a version
            r = filter(lambda el: version + el not in r, r)
        return list(r)

    def xmlrpc_col(self, *a, **k):
        r = self.col(*a, **k)
        return csutil.binfunc(dumps(r))

    def xmlrpc_col_at(self, *a, **k):
        r = self.col_at(*a, **k)
        return csutil.binfunc(dumps(r))

    @lockme()
    def get_decoded(self, path, idx, get):
        """Get the `path` node index `idx` using the getter function `get`"""
        n = self._get_node(path)
        r = get(n, idx)
#		n.close()
        return r

    @lockme()
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
        self.log.debug(startTime, si, endTime, ei)
        arr = n[si:ei]
#		n.close()
        if step is None:
            return arr
        # Interpolate for time stepping
        st = t[si]
        et = t[ei]
        ts = np.arange(st, et, step)
        self.log.debug('tseq', st, et, step, ts)
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
            instr = getattr(self.conf, self.instrument_name(), None)
        if instr is None:
            self.log.debug('Impossible to run scripts: conf is not available.')
            return False
        if self.conf.kiln is not None:
            instr.kiln = self.conf.kiln
        # Associate scripts to their output Meta options
        instr.outFile = self
        instr.distribute_scripts(self)
        instr.characterization(period='all')
        return True

    def copy(self):
        return self

    def connect(self):
        return True

    def has_key(self, *a, **k):
        return False

    def __contains__(self, k):
        return False

    def decode(self, method):
        """Return if a method name should be decoded client-side"""
        return hasattr(self, 'xmlrpc_' + method)
