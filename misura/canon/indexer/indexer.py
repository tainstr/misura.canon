# -*- coding: utf-8 -*-
"""Indexing hdf5 files"""
# NOTICE: THIS FILE IS ALSO PART OF THE CLIENT. IT SHOULD NOT CONTAIN
# REFERENCES TO THE SERVER OR TWISTED PKG.
ext = '.h5'

import hashlib
import os
from time import time
try:
    import cPickle as pickle
except:
    import pickle
    unicode = str
from traceback import print_exc
import sqlite3
import functools
import threading
import multiprocessing
import datetime

from misura.canon.csutil import unlockme, lockme, enc_options, sharedProcessResources

import tables
from tables.nodes import filenode

from .. import csutil, option

from .filemanager import FileManager
from misura.canon.indexer.interface import SharedFile
from misura.canon.plugin import NullTasks

testColumn = ('file', 'serial', 'uid', 'id', 'zerotime', 'instrument',
              'flavour', 'name', 'elapsed', 'nSamples', 'comment', 'verify')

col_uid = testColumn.index('uid')
col_serial = testColumn.index('serial')


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
modifyDatesTableDef = '''(modify_date integer, file text unique)'''

syncTableDef = '''(file text, serial text, uid text primary key, id text,
                   zerotime text, instrument text, flavour text, name text,
                   elapsed real, nSamples integer, comment text,verify bool)'''
errorTableDef = syncTableDef[:-1] + ', error text)'

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

def convert_to_relative_path(file_path, dbdir):
    if file_path.startswith('.'):
        return file_path
    relative_path = "." + \
        file_path[len(dbdir):] if file_path.startswith(
            dbdir) else file_path
    relative_path = '/'.join(relative_path.split(os.sep))
    return relative_path


class FileSystemLock(object):
    stale_file_timeout = 10

    def __init__(self,  path=False):
        self._lock = multiprocessing.Lock()
        self.path = path
        sharedProcessResources.register(self.restore_lock, self._lock)

    def restore_lock(self, lk):
        print('FileSystemLock.restore_lock')
        self._lock = lk
        
    def __getstate__(self):
        r = self.__dict__.copy()
        r.pop('_lock')
        return r
    
    def __setstate__(self, s):
        self.__dict__ = s
        self._lock = multiprocessing.Lock()
        
    

    def set_path(self, path):
        self.path = path
        if path:
            self.path += '.lock'

    def acquire(self,  block=True,  timeout=0):
        r = self._lock.acquire(block)
        if not r or not self.path:
            return r
        t0 = time()
        try:
            t1 = os.path.getctime(self.path)
        except:
            t1 = t0
        while os.path.exists(self.path):
            if t0 - t1 > self.stale_file_timeout:
                os.rmdir(self.path)
                raise BaseException(
                    'Stale FileSystemLock detected: ' + self.path)
            if not block:
                return False
            if timeout > 0 and (time() - t0) > timeout:
                raise BaseException('Lock timed out')
        os.mkdir(self.path)
        return True

    def release(self):
        r = self._lock.release()
        os.rmdir(self.path)
        return r


class Indexer(object):
    public = ['rebuild', 'searchUID', 'update', 'header', 'listMaterials',
              'query', 'remove_uid', 'get_len', 'list_tests', 'get_dbpath',
              'refresh']
#   cur=False
#   conn=False
    addr = 'LOCAL'

    def __init__(self, dbPath=False, paths=[], log=False):
        self._lock = FileSystemLock()
        self.tasks = NullTasks()
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
            print('Indexer: no dbpath set!')
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
        cur.execute(
            "create table if not exists incremental_ids " + incrementalIdsTableDef)
        cur.execute(
            "create table if not exists modify_dates " + modifyDatesTableDef)

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
    
    aborted = False
    def abort(self):
        """Abort current rebuild/refresh process"""
        self.log.warning('Rebuild/refresh aborted!')
        self.aborted = True

    @unlockme
    def rebuild(self):
        """Completely recreate the SQLite Database indexing all test files."""
        self.aborted = False
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
        cur.execute("DROP TABLE IF EXISTS modify_dates")
        conn.commit()
        self.close_db()
        self._lock.release()
        tests_filenames = self.tests_filenames_sorted_by_date()
        self.tasks.jobs(len(tests_filenames),'Rebuilding database', abort=self.abort)
        for i, f in enumerate(tests_filenames):
            if self.aborted:
                return 'Aborted. Indexed %i tests.' % i
            self.appendFile(f, False)
            self.tasks.job(i,'Rebuilding database', f)

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
        r = False
        table = False
        try:
            self.log.debug('Appending',  file_path)
            table = tables.open_file(file_path, mode='r')
            if not getattr(table.root, 'conf', False):
                self.log.debug('Tree configuration not found', file_path)
                table.close()
                return False
            r = self._appendFile(
                table, file_path, add_uid_to_incremental_ids_table)
        except:
            print_exc()
        if table:
            table.close()
        return r
    
    @property
    def dbdir(self):
        return os.path.dirname(self.dbPath)

    def get_test_data(self, table, file_path, add_uid_to_incremental_ids_table):
        conf = getattr(table.root, 'conf', False)
        if '/userdata' in table:
            active_version = str(
                table.get_node_attr('/userdata', 'active_version')).strip()
            if active_version:
                version_node = getattr(table.root, active_version, False)
                if version_node is not False:
                    versioned_conf = getattr(version_node, 'conf', False)
                    if versioned_conf is not False:
                        conf = version_node.conf

        # Load configuration
        node = filenode.open_node(conf, 'r')
        node.seek(0)
        tree = node.read()
        node.close()
        opt = enc_options.copy()
        if 'encoding' in opt:
            opt['encoding'] = 'latin1'
        tree = pickle.loads(tree, **opt)

        # ##
        # Test row
        # ##
        test = {}
        
        relative_path = convert_to_relative_path(file_path, self.dbdir)
        test['file'] = relative_path

        instrument = str(conf.attrs.instrument, **enc_options)

        test['instrument'] = instrument
        if instrument not in tree:
            self.log.debug('Instrument tree missing', instrument)
            return False

        for p in 'name,comment,nSamples,zerotime,elapsed,id'.split(','):
            test[p] = tree[instrument]['measure']['self'][p]['current']
        zerotime = test['zerotime']
        test['serial'] = str(conf.attrs.serial, **enc_options)
        uid = getattr(conf.attrs, 'uid', False)
        if not uid or len(uid) < 2:
            self.log.debug('UID attribute not found')
            sname = tree[instrument]['measure']['id']
            test['uid'] = hashlib.md5(
                '%s_%s_%i' % (test['serial'], test['zerotime'], sname)).hexdigest()
        else:
            test['uid'] = str(uid, **enc_options)
        test['flavour'] = 'Standard'
        v = []
        for k in 'file,serial,uid,id,zerotime,instrument,flavour,name,elapsed,nSamples,comment'.split(','):
            v.append(testColConverter[k](test[k]))
#       ok=digisign.verify(table)
        # Performance problem: should be only verified on request.
        ok = False
        print('File verify:', ok)
        v.append(ok)
        return v, tree, instrument, test

    def save_modify_date(self, file_name):
        full_test_file_name = self.convert_to_full_path(file_name)
        modify_date = int(os.path.getmtime(full_test_file_name))
        
        query = "INSERT OR REPLACE INTO modify_dates VALUES (?, ?)"

        self.cur.execute(query, (modify_date, file_name))
        r = self.cur.fetchall()

        self.conn.commit()

    @dbcom
    def get_modify_dates(self):
        query = "select * from modify_dates"
        self.cur.execute(query)
        return self.cur.fetchall()

    @dbcom
    def _appendFile(self, table, file_path, add_uid_to_incremental_ids_table):
        """Inserts a new file in the database"""
        v, tree, instrument, test = self.get_test_data(
            table, file_path, add_uid_to_incremental_ids_table)

        cur = self.cur
        cmd = '?,' * len(v)
        cmd = 'INSERT INTO test VALUES (' + cmd[:-1] + ')'
        print('Executing', cmd, v)
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
                if k in smp:
                    val = smp[k]['current']
                v.append(val)
            cmd = '?,' * len(v)
            cmd = 'INSERT INTO sample VALUES (' + cmd[:-1] + ')'
            cur.execute(cmd, v)
            r = cur.fetchall()
            print('Result:', r)

        if add_uid_to_incremental_ids_table:
            self.add_incremental_id(cur, test['uid'])
        self.conn.commit()
        # This is actually the relative path saved before
        self.save_modify_date(test['file'])

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
            cursor.execute(
                "insert into incremental_ids(uid) values (?)", (uid,))
        except sqlite3.IntegrityError:
            self.log.debug(
                'uid ' + uid + ' already exists in incremental_ids table: no big deal')

    def change_column(self,
                      column_name,
                      update_function,
                      new_value,
                      uid,
                      hdf_file_name):

        hdf_file = SharedFile(path=hdf_file_name, uid=uid)
        hdf_file.set_version()
        if hdf_file.get_version() == '':
            hdf_file.create_version()

        instrument_name = str(hdf_file.test.root.conf.attrs.instrument, **enc_options)
        getattr(hdf_file.conf, instrument_name).measure[
            column_name] = new_value

        hdf_file.save_conf()
        hdf_file.close()

        return update_function(new_value, uid)
    
    def change_filename(self, original_file_path, new_filename, uid):
        """Change filename and update db (not directory)"""
        folder = os.path.dirname(original_file_path)
        new_filename = os.path.basename(new_filename)
        if new_filename.endswith(ext):
            new_filename = new_filename[:-len(ext)]
        num = ''
        i = -1
        while True:
            path = os.path.join(folder, new_filename+num+ext)
            if not os.path.exists(path):
                break
            i += 1 
            num = '_{}'.format(i)
        self.log.debug('change_filename rename', original_file_path, path)
        # Rename the file
        os.rename(original_file_path, path)
        
        # Update the db
        relative_path = convert_to_relative_path(path, self.dbdir)
        self.change_column_on_database('file', relative_path, uid)
        return path

    def change_name(self, new_name, uid, hdf_file_name):
        if not new_name:
            return 0
        
        # Rename the hdf5 file
        hdf_file_name = self.change_filename(hdf_file_name, new_name, uid)
        
        return self.change_column('name',
                                  self.change_name_on_database,
                                  new_name,
                                  uid,
                                  hdf_file_name)

    def change_comment(self, new_comment, uid, hdf_file_name):
        return self.change_column('comment',
                                  self.change_comment_on_database,
                                  new_comment,
                                  uid,
                                  hdf_file_name)

    @dbcom
    def change_column_on_database(self, column_name, new_value, uid):
        self.cur.execute(
            "update test set " + column_name + " = ? where uid = ?", (new_value, uid,))
        self.conn.commit()
        return len(self.cur.fetchall())

    def change_name_on_database(self, new_name, uid):
        return self.change_column_on_database('name', new_name, uid)

    def change_comment_on_database(self, new_comment, uid):
        return self.change_column_on_database('comment', new_comment, uid)

    def refresh(self):
        """Updates the database by inserting new files and removing deleted files"""
        self.aborted = False
        self.tasks.jobs(5, 'Refreshing database', abort=self.abort)
        database = self.execute_fetchall("select file, uid from test")
        if self.aborted:
            return False
        self.tasks.job(1, 'Refreshing database', 'Deleting non-existent files')
        database = self.delete_not_existing_files(database)
        if self.aborted:
            return False
        self.tasks.job(2, 'Refreshing database', 'Checking modification dates')
        all_files = self.tests_filenames_sorted_by_date()
        if self.aborted:
            return False
        self.tasks.job(3, 'Refreshing database', 'Forget modified files')
        database = self.delete_modified_files(database, all_files)
        if self.aborted:
            return False
        self.tasks.job(4, 'Refreshing database', 'Re-index modified files')
        self.add_new_files(all_files, database)
        self.tasks.done('Refreshing database')

    def delete_modified_files(self, database, all_files):
        modified_dates_on_db = self.get_modify_dates()
        f = lambda db_entry: (db_entry[0], self.convert_to_full_path(db_entry[1]), db_entry[1])
        modified_dates_on_db = [f(m) for m in modified_dates_on_db]

        old_modified_dates = {}
        for f in all_files:
            old_modified_dates[f] = int(os.path.getmtime(f))
            
        deleted = []
        out = []
        for modify_date_on_db, full_test_file_name, relative_test_file_name in modified_dates_on_db:
            if self.aborted:
                return out
            old_modified_date = old_modified_dates.get(
                full_test_file_name, False)
            if old_modified_date and old_modified_date != modify_date_on_db:
                # Remove so it will be later re-added
                self.clear_file_path(relative_test_file_name)
                deleted.append(relative_test_file_name)
        
        
        for relative_file_path, uid in database:
            if relative_file_path not in deleted:
                out.append([relative_file_path, uid])
        return out

    def delete_not_existing_files(self, database):
        out = []
        for i, (relative_file_path, uid) in enumerate(database):
            if self.aborted:
                return out
            absolute_file_path = self.convert_to_full_path(relative_file_path)
            if not os.path.exists(absolute_file_path):
                self.clear_file_path(relative_file_path)
            else:
                out.append([relative_file_path, uid])
        return out

    def add_new_files(self, all_files, database):
        file_names_in_database = [self.convert_to_full_path(d[0]) for d in database]
        pid = 'Adding files'
        self.tasks.jobs(len(all_files), pid, abort=self.abort)
        for i, f in enumerate(all_files):
            if self.aborted:
                return False
            msg = ''
            if f not in file_names_in_database:
                r = self.appendFile(f)
                msg = f
            self.tasks.job(i, pid, msg)
        self.tasks.done(pid)
        return True

    def remove_uid(self, uid):
        fn = self.searchUID(uid)
        if not fn:
            self.log.error('Impossible delete:', uid, 'not found.')
            return False
        self.log.debug('Removing file by uid:', uid, fn)
        return self.remove_file(fn)

    def _clear_file_path(self, relative_file_path):
        """Remove file in `relative_file_path` from db"""
        self.log.debug(
            'Removing obsolete database entry: ', relative_file_path)
        e = self.cur.execute(
            'delete from test where file=?', (relative_file_path,))
        e = self.cur.execute(
            'delete from sample where file=?', (relative_file_path,))
        self.conn.commit()
        return True

    @dbcom
    def clear_file_path(self, relative_file_path):
        """Implicit connection and locked version of _clear_file_path"""
        self._clear_file_path(relative_file_path)

    @dbcom
    def remove_file(self, file_path):
        self._clear_file_path(file_path)
        if os.path.exists(file_path):
            self.log.debug('Removing existing data file:', file_path)
            os.remove(file_path)
            return True
        else:
            self.log.info('Asked to delete a non-existing file:', file_path)
            return False

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
        print('LIST MATERIALS', r)
        return r

    @dbcom
    def query(self, conditions={}, operator=1, orderby='zerotime', order='DESC', limit=1000, offset=0):
        # FIXME: inter-thread #412
        operator = ['OR', 'AND'][operator]
        order = order.upper()
        assert order in ('DESC', 'ASC')
        assert orderby in testColumnDefault
        limit = int(limit)
        assert limit>0
        offset = int(offset)
        assert offset>=0
        ordering = " ORDER BY `{}` {} LIMIT {} OFFSET {}".format(orderby, 
                                                        order, limit, offset)
        if len(conditions) == 0:
            self.cur.execute(
                'SELECT * from test natural join incremental_ids ' + ordering)
        else:
            cnd = []
            vals = []
            for k, v in conditions.items():
                cnd.append(k + ' like ?')
                vals.append('%' + v + '%')
            cnd = ' {} '.format(operator).join(cnd)
            cmd = 'SELECT * from test natural join incremental_ids WHERE ' + \
                cnd + ordering
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
    def list_tests(self):
        self.cur.execute('SELECT * FROM test ORDER BY zerotime')
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
                file_path = '/'.join(file_path.split('\\'))
            relative_path = [self.dbdir] + file_path[1:].split('/')
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
    sys.exit()
    db = Indexer(sys.argv[1])
    db.rebuild()
    db.close()
