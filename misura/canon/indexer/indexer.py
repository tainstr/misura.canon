# -*- coding: utf-8 -*-
"""Indexing hdf5 files"""
# NOTICE: THIS FILE IS ALSO PART OF THE CLIENT. IT SHOULD NOT CONTAIN
# REFERENCES TO THE SERVER OR TWISTED PKG.
ext = '.h5'

import hashlib
import os
import cPickle as pickle
from traceback import print_exc
import sqlite3
import functools
import threading

from misura.canon.csutil import unlockme

import tables
from tables.nodes import filenode

from .. import csutil, option

from filemanager import FileManager

testColumn = ('file', 'serial', 'uid', 'id', 'date', 'instrument',
              'flavour', 'name', 'elapsed', 'nSamples', 'comment', 'verify')
testColumnDefault = ['file', 'serial', 'uid', 'id', 'date',
                     'instrument', 'flavour', 'name', 1, 1, 'comment', 0]
testColDef = ('text', 'text', 'text', 'text', 'text', 'text',
              'text', 'text', 'real', 'integer', 'text', 'bool')
testTableDef = '''(file text unique, serial text, uid text primary key, id text, date text, instrument text, flavour text,
		name text, elapsed real, nSamples integer, comment text,verify bool)'''
syncTableDef = '''(file text, serial text, uid text primary key, id text, date text, instrument text, flavour text,
		name text, elapsed real, nSamples integer, comment text,verify bool)'''
errorTableDef = '''(file text, serial text, uid text, id text, date text, instrument text, flavour text,
		name text, elapsed real, nSamples integer, comment text,verify bool,error text)'''
sampleTableDef = '''(file text, ii integer, idx integer, material text, name text, comment text,
		dim integer, height integer, volume integer,
		sintering real, softening real, sphere real, halfSphere real, melting real )'''
testColConverter = {}
colConverter = {'text': unicode, 'real': float, 'bool': bool, 'integer': int}
for i, n in enumerate(testColumn):
    testColConverter[n] = colConverter[testColDef[i]]


def dbcom(func):
    """Decorator to open db before operations and close at the end."""
    @functools.wraps(func)
    def safedb_wrapper(self, *args, **kwargs):
        try:
            r = self._lock.acquire(False)
            if not r:
                raise BaseException('Impossible to lock database')
            self.open_db()
            return func(self, *args, **kwargs)
        finally:
            try:
                self.close_db()
            except:
                print_exc()
            finally:
                self._lock.acquire(False)
                try:
                    self._lock.release()
                except:
                    print_exc()
    return safedb_wrapper


def tid():
    return threading.current_thread().ident


class Indexer(object):
    public = ['rebuild', 'searchUID', 'update', 'header', 'listMaterials',
              'query', 'remove', 'get_len', 'list_tests', 'get_dbpath']
# 	cur=False
# 	conn=False
    addr = 'LOCAL'

    def __init__(self, dbPath=False, paths=[], log=False):
        self._lock = threading.Lock()
        self.threads = {}
        self.dbPath = dbPath
        self.paths = paths
        if log is False:
            log = csutil.FakeLogger()
        self.log = log
        self.test = FileManager(self)
        self.dbPath = dbPath
        if dbPath and not os.path.exists(dbPath):
            self.rebuild()

    @dbcom
    def execute(self, query, *a, **kw):
        self.cur.execute(query, *a, **kw)
        self.conn.commit()

    @dbcom
    def execute_fetchall(self, query, *a, **kw):
        g = self.cur.execute(query, *a, **kw)
        return g.fetchall()

    @dbcom
    def execute_fetchone(self, query, *a, **kw):
        g = self.cur.execute(query, *a, **kw)
        return g.fetchone()

    @property
    def conn(self):
        return self.threads.get(tid(), [False])[0]

    @property
    def cur(self):
        return self.threads.get(tid(), [False])[-1]

    def open_db(self, db=False):
        if not db:
            db = self.dbPath
        self.dbPath = db
        if not self.dbPath:
            print 'Indexer: no dbpath set!'
            return False

        conn, cur = self.threads.get(tid(), (False, False))
        if conn:
            try:
                cur = conn.cursor()
            except sqlite3.ProgrammingError:
                conn = False
        if not conn:
            conn = sqlite3.connect(
                self.dbPath, detect_types=sqlite3.PARSE_DECLTYPES)
            cur = conn.cursor()
            self.threads[tid()] = (conn, cur)

        cur.execute("CREATE TABLE if not exists test " + testTableDef)
        cur.execute("CREATE TABLE if not exists sample " + sampleTableDef)
        # Sync tables
        cur.execute("create table if not exists sync_exclude " + syncTableDef)
        cur.execute("create table if not exists sync_queue " + syncTableDef)
        cur.execute("create table if not exists sync_approve " + syncTableDef)
        cur.execute("create table if not exists sync_error " + errorTableDef)
        conn.commit()
        return True

    def close_db(self):
        conn, cur = self.threads.get(tid(), (0, 0))
        if cur:
            cur.close()
        if conn:
            conn.close()
        return True

    def close(self):
        return self.close_db()

    def tab_len(self, table_name):
        """Returns length of a table"""
        r = self.execute_fetchone('SELECT COUNT(*) from {}'.format(table_name))
        if not r:
            return 0
        return r[0]

    def _searchUID(self, uid, full=False):
        """Unlocked searchUID"""
        result = self.cur.execute('SELECT file FROM test WHERE uid=?', [str(uid)]).fetchall()
        if len(result) == 0:
            return False
        file_path = result[0][0]

        if not os.path.exists(file_path):
            self._clear_file_path(file_path)
            return False

        if full:
            return result[0]
        return file_path

    @dbcom
    def searchUID(self, uid, full=False):
        """Search `uid` in tests table and return its path or full record if `full`"""
        return self._searchUID(uid, full)

    @unlockme
    def rebuild(self):
        """Completely recreate the SQLite Database indexing all test files."""
        if not self.dbPath:
            return False
        if os.path.exists(self.dbPath):
            os.remove(self.dbPath)
        if not self._lock.acquire(False):
            self.log.error('Cannot lock database for rebuild')
            return False
        self.open_db(self.dbPath)

        conn = self.conn
        cur = self.cur
        cur.execute("DROP TABLE IF EXISTS test")
        cur.execute("CREATE TABLE test " + testTableDef)
        cur.execute("DROP TABLE IF EXISTS sample")
        cur.execute("CREATE TABLE sample " + sampleTableDef)
        cur.execute("DROP TABLE IF EXISTS sync_exclude")
        cur.execute("DROP TABLE IF EXISTS sync_queue")
        cur.execute("DROP TABLE IF EXISTS sync_approve")
        cur.execute("DROP TABLE IF EXISTS sync_error")
        conn.commit()
        self.close_db()
        self._lock.release()
        tn = 0
        for path in self.paths:
            for root, dirs, files in os.walk(path):
                for fn in files:
                    if not fn.endswith(ext):
                        continue
                    fp = os.path.join(root, fn)
                    print 'Appending', fp
                    tn += self.appendFile(fp, fn)

        return 'Done. Found %i tests.' % tn

    def appendFile(self, fp, fn=False):
        # TODO: remove unused fn argument!!!
        if not os.path.exists(fp):
            print 'File not found', fp
            return False
        if not fn:
            fn = os.path.basename(fp)
        r = 0
        t = False
        try:
            t = tables.openFile(fp, mode='r+')
            if not getattr(t.root, 'conf', False):
                self.log.debug('Tree configuration not found', fp)
                t.close()
                return False
            r = self._appendFile(t, fp, fn)
        except:
            print_exc()
        if t:
            t.close()
        return r

    @dbcom
    def _appendFile(self, t, fp, fn):
        """Inserts a new file in the database"""
        # FIXME: inter-thread #412
        cur = self.cur
        conf = getattr(t.root, 'conf', False)
        # Load configuration
        node = filenode.openNode(t.root.conf, 'r')
        node.seek(0)
        tree = node.read()
        node.close()
        tree = pickle.loads(tree)

        ###
        # Test row
        ###
        test = {}
        test['file'] = fp
        instrument = conf.attrs.instrument
        test['instrument'] = instrument
        if not tree.has_key(instrument):
            print tree.keys()
            self.log.debug('Instrument tree missing')
            return False

        for p in 'name,comment,nSamples,date,elapsed,id'.split(','):
            test[p] = tree[instrument]['measure']['self'][p]['current']
        if test['date'] <= 1:
            test['date'] = os.stat(fp).st_ctime
        test['serial'] = conf.attrs.serial
        if not getattr(conf.attrs, 'uid', False):
            self.log.debug('UID attribute not found')
            sname = tree[instrument]['measure']['id']
            test['uid'] = hashlib.md5(
                '%s_%s_%i' % (test['serial'], test['date'], sname)).hexdigest()
        else:
            test['uid'] = conf.attrs.uid
        test['flavour'] = 'Standard'
        v = []
        for k in 'file,serial,uid,id,date,instrument,flavour,name,elapsed,nSamples,comment'.split(','):
            v.append(testColConverter[k](test[k]))
# 		ok=digisign.verify(t)
        # Performance problem: should be only verified on request.
        ok = False
        print 'File verify:', ok
        v.append(ok)

        # Remove any old entry
        self._clear_file_path(fn)

        cmd = '?,' * len(v)
        cmd = 'INSERT INTO test VALUES (' + cmd[:-1] + ')'
        print 'Executing', cmd, v
        self.cur.execute(cmd, v)
        r = cur.fetchall()
        ###
        # Sample Rows
        ###
        for i in range(8):
            s = 'sample%i' % i
            if s not in tree[instrument].keys():
                break
            smp = tree[instrument][s]
            v = [fp]
            for k in 'ii,index,material,name,comment,dim,height,volume,sintering,softening,sphere,halfSphere,melting'.split(','):
                val = 0
                if smp.has_key(k):
                    val = smp[k]['current']
                v.append(val)
            cmd = '?,' * len(v)
            cmd = 'INSERT INTO sample VALUES (' + cmd[:-1] + ')'
            cur.execute(cmd, v)
            r = cur.fetchall()
            print 'Result:', r
        self.conn.commit()

        ###
        # Options
        ###
        return True
        s = option.SqlStore()
        s.cursor = cur
        s.write_tree(tree, preset=test['uid'])
        self.conn.commit()
        return True

    def update(self):
        """Updates the database by inserting new files and removing deleted files"""
        # TODO
        pass

    @dbcom
    def remove(self, uid):
        fn = self._searchUID(uid)
        if not fn:
            self.log.error('Impossible delete:', uid, 'not found.')
            return False
        return self.remove_file(fn)

    def _clear_file_path(self, file_path):
        """Remove file in `file_path` from db"""
        e = self.cur.execute('delete from test where file=?', (file_path,))
        print 'Deleted from test', e.rowcount
        e = self.cur.execute('delete from sample where file=?', (file_path,))
        print 'Deleted from sample', e.rowcount
        self.conn.commit()
        return True

    @dbcom
    def remove_file(self, file_path):
        r = self._clear_by_file_path(file_path)
        if os.path.exists(file_path):
            os.remove(file_path)
        return r

    @dbcom
    def header(self):
        self.cur.execute('PRAGMA table_info(test)')
        r = self.cur.fetchall()
        print 'Indexer.header', r
        return [str(col[1]) for col in r]

    @dbcom
    def listMaterials(self):
        """Lists all materials present in the database"""
        self.cur.execute('SELECT material FROM sample')
        r = self.cur.fetchall()
        r = list(a[0] for a in r)
        r = list(set(r))
        print 'LIST MATERIALS', r
        return r

    @dbcom
    def query(self, conditions={}):
        # FIXME: inter-thread #412
        if len(conditions) == 0:
            self.cur.execute('SELECT * from test')
        else:
            cnd = []
            vals = []
            for k, v in conditions.iteritems():
                cnd.append(k + ' like ?')
                vals.append('%' + v + '%')
            cnd = ' AND '.join(cnd)
            cmd = 'SELECT * from test WHERE ' + cnd
            self.log.debug('Executing', cmd, vals)
            self.cur.execute(cmd, vals)
        r = self.cur.fetchall()
        return r

    @dbcom
    def get_len(self):
        self.cur.execute('SELECT Count(*) FROM test')
        r = self.cur.fetchone()[0]
        return r

    @dbcom
    def list_tests(self, start=0, stop=25):
        self.cur.execute(
            'SELECT * FROM test ORDER BY rowid DESC LIMIT ? OFFSET ?', (stop - start, start))
        r = []
        for record in self.cur.fetchall():
            file_path = record[0]
            if not os.path.exists(file_path):
                self.log.debug('Removing obsolete database entry: ', file_path)
                self._clear_file_path(file_path)
            else:
                r.append(record)
        return r

    def get_dbpath(self):
        return self.dbPath
