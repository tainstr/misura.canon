# -*- coding: utf-8 -*-
"""Indexing hdf5 files"""
# NOTICE: THIS FILE IS ALSO PART OF THE CLIENT. IT SHOULD NOT CONTAIN
# REFERENCES TO THE SERVER OR TWISTED PKG.
ext = '.h5'

import hashlib
import os
from time import time, sleep
import cPickle as pickle
from traceback import print_exc
import sqlite3
import functools
import threading
import multiprocessing
import datetime

from misura.canon.csutil import unlockme

import tables
from tables.nodes import filenode

from .. import csutil, option

from filemanager import FileManager
from misura.canon.indexer.interface import SharedFile

testColumn = ('file', 'serial', 'uid', 'id', 'zerotime', 'instrument',
              'flavour', 'name', 'elapsed', 'nSamples', 'comment', 'verify')

columns_to_translate = testColumn + ('incremental_id',)

testColumnDefault = ['file', 'serial', 'uid', 'id', 'zerotime',
                     'instrument', 'flavour', 'name', 1, 1, 'comment', 0]
testColDef = ('text', 'text', 'text', 'text', 'date', 'text',
              'text', 'text', 'real', 'integer', 'text', 'bool')
testTableDef = '''(file text unique, serial text, uid text primary key,
                   id text, zerotime text, instrument text, flavour text,
                   name text, elapsed real, nSamples integer,
                   comment text,verify bool)'''
incrementalIdsTableDef = '''(incremental_id INTEGER PRIMARY KEY AUTOINCREMENT, uid text unique)'''

syncTableDef = '''(file text, serial text, uid text primary key, id text,
                   zerotime text, instrument text, flavour text, name text,
                   elapsed real, nSamples integer, comment text,verify bool)'''
errorTableDef = '''(file text, serial text, uid text, id text, zerotime date,
                    instrument text, flavour text, name text, elapsed real,
                    nSamples integer, comment text,verify bool,error text)'''
sampleTableDef = '''(file text, ii integer, idx integer, material text,
                     name text, comment text, dim integer, height integer,
                     volume integer, sintering real, softening real,
                     sphere real, halfSphere real, melting real )'''
testColConverter = {}
colConverter = {'text': unicode, 'real': float, 'bool': bool, 'integer': int,
                'date': lambda x: str(datetime.datetime.fromtimestamp(int(x)))}

for i, n in enumerate(testColumn):
    testColConverter[n] = colConverter[testColDef[i]]


def dbcom(func):
    """Decorator to open db before operations and close at the end."""
    @functools.wraps(func)
    def safedb_wrapper(self, *args, **kwargs):
        try:
            r = self._lock.acquire(timeout=10)
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

class FileSystemLock(object):
    stale_file_timeout = 10
    
    def __init__(self,  path=False):
        self._lock = multiprocessing.Lock()
        self.path = path
        
    def set_path(self, path):
        self.path = path+'.lock'
        
    def acquire(self,  block=True,  timeout=0):
        r=self._lock.acquire(block)
        if not r or not self.path:
            return r
        t0 = time()
        while os.path.exists(self.path):
            if t0-os.path.getctime(self.path) > self.stale_file_timeout:
                os.rmdir(self.path)
                raise BaseException('Stale FileSystemLock detected: ' + self.path)
            if not block:
                return False
            if timeout>0 and (time()-t0)>timeout:
                raise BaseException('Lock timed out')
        os.mkdir(self.path)
        return True
        
    def release(self):
        if not self.path:
            return self._lock.release()
        if not os.path.exists(self.path):
            self._lock.release()
            raise BaseException('Releasing a released lock')
        os.rmdir(self.path)
        return self._lock.release()
        


class Indexer(object):
    public = ['rebuild', 'searchUID', 'update', 'header', 'listMaterials',
              'query', 'remove', 'get_len', 'list_tests', 'get_dbpath']
#   cur=False
#   conn=False
    addr = 'LOCAL'

    def __init__(self, dbPath=False, paths=[], log=False):
        self._lock = FileSystemLock()
        self.threads = {}
        self.dbPath = dbPath
        self.paths = paths
        if log is False:
            log = csutil.FakeLogger()
        self.log = log
        self.test = FileManager(self)
        self._lock.set_path(dbPath)
        if dbPath and not os.path.exists(dbPath):
            self.rebuild()

    @classmethod
    def append_file_to_database(cls, dbpath, filepath):
        """Append `filepath` to database in `dbpath`."""
        db = Indexer(dbpath, [])
        r = db.appendFile(filepath)
        db.close()
        return r

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
        self._lock.set_path(db)
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

        cur.execute("create table if not exists incremental_ids " + incrementalIdsTableDef)
        conn.commit()
        return True

    def close_db(self):
        conn, cur = self.threads.pop(tid(), (0, 0))
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
        result = self.cur.execute(
            'SELECT file FROM test WHERE uid=?', [str(uid)]).fetchall()
        if len(result) == 0:
            return False

        file_path = self.convert_to_full_path(result[0][0])

        if not os.path.exists(file_path):
            self._clear_file_path(result[0][0])
            return False

        if full:
            return (file_path,) + result[0][1:]
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
        # if os.path.exists(self.dbPath):
        #     os.remove(self.dbPath)
        if not self._lock.acquire(timeout=10):
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
        tests_filenames = self.tests_filenames_sorted_by_date()

        for f in tests_filenames:
            self.appendFile(f, False)

        self.recalculate_incremental_ids()

        return 'Done. Found %i tests.' % len(tests_filenames)

    @dbcom
    def recalculate_incremental_ids(self):
        self.cur.execute("select uid from test order by zerotime")
        uids = self.cur.fetchall()
        for uid in uids:
            self.add_incremental_id(self.cur, uid[0])

        self.conn.commit()

    def tests_filenames_sorted_by_date(self):
        tests_filenames = []

        for path in self.paths:
            for root, dirs, files in os.walk(path):
                for fn in files:
                    if not fn.endswith(ext):
                        continue
                    fp = os.path.join(root, fn)
                    tests_filenames.append(fp)

        return sorted(tests_filenames, key=os.path.getctime)

    def appendFile(self, file_path, add_uid_to_incremental_ids_table=True):
        if not os.path.exists(file_path):
            self.log.warning('File not found', file_path)
            return False
        r = 0
        table = False
        try:
            self.log.debug('Appending',  file_path)
            table = tables.openFile(file_path, mode='r+')
            if not getattr(table.root, 'conf', False):
                self.log.debug('Tree configuration not found', file_path)
                table.close()
                return False
            r = self._appendFile(table, file_path, add_uid_to_incremental_ids_table)
        except:
            print_exc()
        if table:
            table.close()
        return r

    @dbcom
    def _appendFile(self, table, file_path, add_uid_to_incremental_ids_table):
        """Inserts a new file in the database"""
        # FIXME: inter-thread #412
        cur = self.cur
        conf = getattr(table.root, 'conf', False)
        if '/userdata' in table:
            active_version = str(table.get_node_attr('/userdata', 'active_version')).strip()
            if active_version:
                version_node = getattr(table.root, active_version, False)
                if version_node is not False:
                    conf = version_node.conf

        # Load configuration
        node = filenode.openNode(conf, 'r')
        node.seek(0)
        tree = node.read()
        node.close()
        tree = pickle.loads(tree)

        # ##
        # Test row
        # ##
        test = {}

        dbdir = os.path.dirname(self.dbPath)
        relative_path = "." + \
            file_path[len(dbdir):] if file_path.startswith(
                dbdir) else file_path
        relative_path = '/'.join(relative_path.split(os.sep))
        test['file'] = relative_path

        instrument = conf.attrs.instrument

        test['instrument'] = instrument
        if not tree.has_key(instrument):
            print tree.keys()
            self.log.debug('Instrument tree missing')
            return False

        for p in 'name,comment,nSamples,zerotime,elapsed,id'.split(','):
            test[p] = tree[instrument]['measure']['self'][p]['current']
        zerotime = test['zerotime']
        test['serial'] = conf.attrs.serial
        if not getattr(conf.attrs, 'uid', False):
            self.log.debug('UID attribute not found')
            sname = tree[instrument]['measure']['id']
            test['uid'] = hashlib.md5(
                '%s_%s_%i' % (test['serial'], test['zerotime'], sname)).hexdigest()
        else:
            test['uid'] = conf.attrs.uid
        test['flavour'] = 'Standard'
        v = []
        for k in 'file,serial,uid,id,zerotime,instrument,flavour,name,elapsed,nSamples,comment'.split(','):
            v.append(testColConverter[k](test[k]))
#       ok=digisign.verify(table)
        # Performance problem: should be only verified on request.
        ok = False
        print 'File verify:', ok
        v.append(ok)

        cmd = '?,' * len(v)
        cmd = 'INSERT INTO test VALUES (' + cmd[:-1] + ')'
        print 'Executing', cmd, v
        self.cur.execute(cmd, v)
        r = cur.fetchall()
        # ##
        # Sample Rows
        # ##
        for i in range(8):
            s = 'sample%i' % i
            if s not in tree[instrument].keys():
                break
            smp = tree[instrument][s]
            v = [file_path]
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

        if add_uid_to_incremental_ids_table:
            self.add_incremental_id(cur, test['uid'])
        self.conn.commit()

        # ##
        # Options
        # ##
        return True
        s = option.SqlStore()
        s.cursor = cur
        s.write_tree(tree, preset=test['uid'])
        self.conn.commit()
        return True

    def add_incremental_id(self, cursor, uid):
        try:
            cursor.execute("insert into incremental_ids(uid) values (?)", (uid,))
        except sqlite3.IntegrityError:
            self.log.debug('uid ' + uid + ' already exists in incremental_ids table: no big deal')

    def change_name(self, new_name, uid, hdf_file_name):
        if not new_name:
            return 0
        hdf_file = SharedFile(path=hdf_file_name, uid=uid)
        active_version = hdf_file.active_version()
        if active_version:
            hdf_file.set_version(active_version)
        hdf_file.create_version()

        instrument_name = hdf_file.test.root.conf.attrs.instrument
        getattr(hdf_file.conf, instrument_name).measure['name'] = new_name


        hdf_file.save_conf()
        hdf_file.close()

        return self.change_name_on_database(new_name, uid)

    @dbcom
    def change_name_on_database(self, new_name, uid):
        self.cur.execute("update test set name = ? where uid = ?", (new_name, uid,))
        self.conn.commit()
        return len(self.cur.fetchall())


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

    def _clear_file_path(self, relative_file_path):
        """Remove file in `relative_file_path` from db"""
        self.log.debug('Removing obsolete database entry: ', relative_file_path)
        e = self.cur.execute('delete from test where file=?', (relative_file_path,))
        e = self.cur.execute('delete from sample where file=?', (relative_file_path,))
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
        test_header = [str(col[1]) for col in r]

        self.cur.execute('PRAGMA table_info(incremental_ids)')
        r = self.cur.fetchall()
        incremental_ids_header = [str(col[1]) for col in r]

        for h in incremental_ids_header:
            if h not in test_header:
                test_header.append(h)

        return test_header

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
            self.cur.execute('SELECT * from test natural join incremental_ids ORDER BY zerotime DESC')
        else:
            cnd = []
            vals = []
            for k, v in conditions.iteritems():
                cnd.append(k + ' like ?')
                vals.append('%' + v + '%')
            cnd = ' AND '.join(cnd)
            cmd = 'SELECT * from test natural join incremental_ids WHERE ' + cnd + 'ORDER BY zerotime DESC'
            self.log.debug('Executing', cmd, vals)
            self.cur.execute(cmd, vals)
        r = self.cur.fetchall()
        return self.convert_query_result_to_full_path(r)

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
            file_path = self.convert_to_full_path(record[0])

            if not os.path.exists(file_path):
                self._clear_file_path(record[0])
            else:
                record = (file_path,) + record[1:]
                r.append(record)

        return r

    def get_dbpath(self):
        return self.dbPath

    def convert_to_full_path(self, file_path):
        if file_path.startswith("."):
            if file_path.startswith('.\\'):
                file_path='/'.join(file_path.split('\\'))
            dbdir = os.path.dirname(self.dbPath)
            relative_path = [dbdir]+file_path[1:].split('/')
            relative_path = os.path.join(*relative_path)
            return relative_path

        return file_path

    def convert_query_result_to_full_path(self, query_results):
        converted = []
        for query_result in query_results:
            converted.append(
                (self.convert_to_full_path(query_result[0]),) + query_result[1:])

        return converted

if __name__ == '__main__':
    import sys
    print sys.argv[1]
    sys.exit()
    db = Indexer(sys.argv[1])
    db.rebuild()
    db.close()
