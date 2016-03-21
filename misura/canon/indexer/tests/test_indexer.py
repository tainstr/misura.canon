#!/usr/bin/python
# -*- coding: utf-8 -*-
import unittest
from misura.canon import indexer

import os
import shutil
import sqlite3

def ensure_deletion_of_file(file_to_delete):
    def decorator(function_to_decorate):
        def wrapper(*args, **kwargs):
            try:
                retval = function_to_decorate(*args, **kwargs)
            except Exception, e:
                raise e
            finally:
                os.remove(file_to_delete)

            return retval
        return wrapper
    return decorator




cur_dir = os.path.dirname(os.path.realpath(__file__))

paths = [cur_dir + '/files']
dbPath = cur_dir + '/files/test.sqlite'

class Indexer(unittest.TestCase):

    def setUp(self):
        shutil.copyfile(cur_dir + '/files/start-db.sqlite', dbPath)

        self.indexer = indexer.Indexer(paths=paths)
        self.indexer.open_db(dbPath)
        self.indexer.close_db()

    def tearDown(self):
        os.remove(dbPath)

    def test_rebuild(self):
        self.indexer.rebuild()
        self.assertEqual(2, self.indexer.get_len())

    def test_header(self):
        header = self.indexer.header()
        self.assertEqual(['file', 'serial', 'uid', 'id', 'date', 'instrument',
                          'flavour', 'name', 'elapsed', 'nSamples', 'comment', 'verify', 'incremental_id'], header)

    def test_query_returns_all_files(self):
        result = self.indexer.query()
        instrument = result[0][5]

        result = self.indexer.query({'instrument': instrument})
        self.assertEqual(len(result), 1)

        result = self.indexer.query({'instrument': 'pippo'})
        self.assertEqual(len(result), 0)

    def test_searchUID(self):
        actual_path = self.indexer.searchUID('eadd3abc68fa78ad64eb6df7174237a0')
        self.assertEqual(cur_dir + '/files/dummy1.h5', actual_path)

    def test_searchUIDFull(self):
        actual_path = self.indexer.searchUID('eadd3abc68fa78ad64eb6df7174237a0', True)
        self.assertEqual((cur_dir + '/files/dummy1.h5',), actual_path)

    def test_appendFileNonExistingFile(self):
        self.assertFalse(self.indexer.appendFile("non_existing_file"))

    def test_appendFile_should_save_full_path_when_no_relative_is_possible(self):
        self.indexer.appendFile(cur_dir + '/other-files/dummy3.h5')

        conn = sqlite3.connect(dbPath, detect_types=sqlite3.PARSE_DECLTYPES)
        cur = conn.cursor()
        result = cur.execute('SELECT file FROM test WHERE uid=?', ['cd3c070164561106e9b001888edc38fc']).fetchall()
        actual_path = result[0][0]

        self.assertEqual(cur_dir + '/other-files/dummy3.h5', actual_path)

    @ensure_deletion_of_file(cur_dir + '/files/dummy3.h5')
    def test_appendFile_should_save_relative_path_when_possible(self):
        full_h5path = cur_dir + '/files/dummy3.h5'
        shutil.copyfile(cur_dir + '/other-files/dummy3.h5', full_h5path)

        self.indexer.appendFile(full_h5path)

        conn = sqlite3.connect(dbPath, detect_types=sqlite3.PARSE_DECLTYPES)
        cur = conn.cursor()
        result = cur.execute('SELECT file FROM test WHERE uid=?', ['cd3c070164561106e9b001888edc38fc']).fetchall()

        self.assertEqual("./dummy3.h5", result[0][0])

    @ensure_deletion_of_file(cur_dir + '/files/dummy3.h5')
    def test_path_is_always_absolute_when_reading(self):
        full_h5path = cur_dir + '/files/dummy3.h5'
        shutil.copyfile(cur_dir + '/other-files/dummy3.h5', full_h5path)

        self.indexer.appendFile(full_h5path)

        self.assertEqual(full_h5path, self.indexer.searchUID('cd3c070164561106e9b001888edc38fc'))
        self.assertEqual((full_h5path,), self.indexer.searchUID('cd3c070164561106e9b001888edc38fc', True))
        self.assertEqual(full_h5path, self.indexer.list_tests()[0][0])



if __name__ == "__main__":
    unittest.main()
