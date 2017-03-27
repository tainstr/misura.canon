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
        self.conf = False
        CoreFile.__init__(self, *a, **k)
        self.node_cache = {}

    def open_file(self, path=False, uid='', mode='a', title='', header=True, version='', conf=False):
        """opens the hdf file in `path` or `uid`"""
        self.node_cache = {}
        if not path:
            path = self.path
        if not path:
            self.log.debug('No path supplied', path, self.path, uid)
            return False
        if mode == 'w':
            self.log.debug('Creating in write mode', path)
            tables.open_file(path, mode='w', title=title).close()
        if not os.path.exists(path):
            raise RuntimeError("File %s not found." % path)

        try:
            self.log.debug('opening existing file', path, mode)
            self.test = tables.open_file(path, mode=mode)
            self.path = path
        except:
            self.log.error('Error opening file:', format_exc(), path)
            return False
        self.uid = uid
        if header and mode != 'w':
            if not self.has_node('/userdata'):
                self.create_group('/', 'userdata')
                self.set_attributes('/userdata', attrs={'active_version': ''})
            elif not version:
                version = self.get_node_attr('/userdata', 'active_version')
            if self.has_node('/conf'):
                if self.has_node_attr('/conf', 'uid'):
                    self.uid = self.get_node_attr('/conf', 'uid')
                if version != None:
                    self.set_version(version)
            else:
                self.log.info(
                    'No configuration object was found', path, version)
                self.header(refresh=True, version=self.version)
        if self.conf is False:
            self.conf = option.ConfigurationProxy()
        return self.test, self.path

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
            return {}
        if not self._has_node('/conf'):
            return {}
        v = {'': ('Original', self.test.root.conf.attrs.date)}
        # skip 0 and seek a little farer
        latest = 0
        for node in self.test.list_nodes('/'):
            name = node._v_name
            if not name.startswith('ver_'):
                continue
            ver = int(name.split('_')[-1])
            if ver > latest:
                latest = ver
            v[node._v_pathname] = (node._f_getattr('name'),
                                   node._f_getattr('date'))
        self.test.root.conf.attrs.versions = latest
        self.log.debug('returning versions', v)
        return v

    def get_version_by_name(self, name):
        for path, data in self.get_versions().iteritems():
            if data[0] != name:
                continue
            return path
        return False

    def get_latest_version_number(self):
        v = []
        for k in self.get_versions().iterkeys():
            v.append(0 if k == '' else int(k.split('_')[-1]))
        m = max(v)
        return m

    def get_versions_by_date(self):
        v = self.get_versions()
        v = [[key] + list(val) for key, val in v.iteritems()]
        v.sort(key=lambda e: datetime.strptime(e[2]))

    def get_version(self):
        return self.version

    def set_version(self, newversion=-1):
        """Set the current version to `newversion`"""
        # Load the last used version

        if newversion < 0:
            self._lock.acquire()
            if '/userdata' in self.test:
                newversion = self.test.get_node_attr(
                    '/userdata', 'active_version')
                self.log.debug('Found active version', newversion)
            else:
                newversion = getattr(self.test.root.conf.attrs, 'versions', '')
                self.log.debug('Take latest version', newversion)
            self._lock.release()

        if not isinstance(newversion, basestring):
            newversion = '/ver_{}'.format(newversion)

        if self.version == newversion and self.conf:
            self.log.debug('Not changing version!', self.version, newversion)
            return True

        self._change_version(newversion)
        self.load_conf()
        self.header(refresh=True)
        return True

    def _change_version(self, new_version):
        self.log.debug('Changing version to', new_version)
        self.version = str(new_version)
        self._set_attributes(
            '/userdata', attrs={'active_version': new_version})

    def create_version(self, name=False, overwrite=True):
        """Create a new version with `name`. `overwrite` a previous version with same name."""
        newversion = False
        if name:
            newversion = self.get_version_by_name(name)
        if newversion and overwrite:
            self.log.debug(
                'Found version {} saved as {}. Overwriting.'.format(name, newversion))
            latest = int(newversion.split('_')[-1])
            #self.remove_version(newversion, remove_plots=False)
        else:
            latest = self.get_latest_version_number() + 1
            newversion = '/ver_{}'.format(latest)
        
        if not name:
            name = newversion
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
        self.version = newversion
        self._change_version(newversion)
        self.test.flush()
        return newversion

    def remove_version(self, version_path, remove_plots=True):
        if remove_plots:
            self.remove_node(version_path, recursive=True)
            self.log.info('Removed version', version_path)
            self.flush()
            return True
        for node in self.list_nodes():
            if node == 'plots':
                continue
            self.remove_node(version_path + '/' + node)
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
            if render:
                if path + 'render' in self.test:
                    image = self._file_node(path + 'render')
                else:
                    image = False
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
        plots_path = self.version + '/plot'
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

    def conf_tree(self):
        self.log.debug('Loading conf', self.versioned('/conf'))
        tree = self.file_node(self.versioned('/conf'))
        if tree in [False, None]:
            self.log.warning('Configuration node file not found!')
            return '{}'
        # test
        self.log.debug('loading ', len(tree))
        d = loads(tree)
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
        self.filenode_write(self.version + '/conf', obj=tree)
        if self.version != '':
            a = self.get_attributes('/conf')
            self.set_attributes(self.version + '/conf', attrs=a)
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
        data_with_time = np.transpose(np.vstack((time_data, data)))
        dest_path_reference = reference.Array(
            self, newparent, opt=opt, with_summary=False)
        dest_path_reference.append(data_with_time)
        self.flush()
        if path not in self._header['Array']:
            self._header['Array'].append(path)

    def active_version(self):
        return self.get_node_attr('/userdata', 'active_version')

    @lockme()
    def header(self, reference_classes=['Array'], startswith=False, refresh=False, version=False):
        """Returns all available data references"""
        if not version:
            version = self.version
        if refresh or len(self._header) == 0:
            self._header = list_references(self.test.root)
            self.log.debug('References', len(self._header))
        if reference_classes is False:
            reference_classes = self._header.keys()
        r = []
        for k in reference_classes:
            r += self._header.get(k, [])
        if startswith:
            swv = version + startswith
            r = filter(
                lambda el: el.startswith(startswith) or el.startswith(swv), r)
        if not version:
            r = filter(lambda el: not el.startswith('/ver_'), r)
        else:
            # Exclude element with wrong version
            good = lambda el: el.startswith(
                version + '/') or not el.startswith('/ver_')
            r = filter(good, r)
            # Exclude unversioned elements having a version 
            r = filter(lambda el: version + el not in r, r)
        return r

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

    def decode(self, method):
        """Return if a method name should be decoded client-side"""
        return hasattr(self, 'xmlrpc_' + method)
