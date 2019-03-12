#!/usr/bin/python
# -*- coding: utf-8 -*-
import unittest
import os
import shutil
import sqlite3

from tables.file import _open_files

from misura.canon import indexer
from misura.canon.tests import testdir
from misura.canon.indexer import SharedFile


cur_dir = os.path.dirname(os.path.realpath(__file__))
real_test_file = os.path.join(testdir, 'storage', 'hsm_test.h5')

paths = [os.path.join(cur_dir, 'files')]
dbPath = os.path.join(cur_dir, 'files', 'test.sqlite')
start_db = os.path.join(cur_dir,'files','start-db.sqlite')

class Indexer(unittest.TestCase):

    def setUp(self):
        if os.path.exists(dbPath):
            os.remove(dbPath)
        #shutil.copyfile(start_db, dbPath)
        self.indexer = indexer.Indexer(paths=paths)
        self.indexer.open_db(dbPath)
        self.indexer.close_db()
        self.indexer.rebuild()
        

    def tearDown(self):
        self.indexer.close()
        if os.path.exists(dbPath):
            os.remove(dbPath)
        
        _open_files.close_all()
        for f in ['dummy3.h5', 'hsm_test.h5', 'a new name.h5']:
            f = os.path.join(cur_dir,'files', f)
            if os.path.exists(f):
                #SharedFile.close_handlers(f)
                os.remove(f)

    def test_rebuild(self):
        self.indexer.rebuild()
        self.assertEqual(2, self.indexer.get_len())

    def test_header(self):
        header = self.indexer.header()
        self.assertEqual(['file', 'serial', 'uid', 'id', 'zerotime', 'instrument',
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
        self.assertEqual(os.path.join(cur_dir,'files','dummy1.h5'), actual_path)

    def test_searchUIDFull(self):
        actual_path = self.indexer.searchUID('eadd3abc68fa78ad64eb6df7174237a0', True)
        self.assertEqual((os.path.join(cur_dir,'files','dummy1.h5'),), actual_path)

    def test_appendFileNonExistingFile(self):
        self.assertFalse(self.indexer.appendFile("non_existing_file"))

    def test_appendFile_should_save_full_path_when_no_relative_is_possible(self):
        self.indexer.appendFile(os.path.join(cur_dir,'other-files','dummy3.h5'))

        conn = sqlite3.connect(dbPath, detect_types=sqlite3.PARSE_DECLTYPES)
        cur = conn.cursor()
        result = cur.execute('SELECT file FROM test WHERE uid=?',
                             ['cd3c070164561106e9b001888edc38fc']).fetchall()
        actual_path = result[0][0].replace('/', os.sep)

        self.assertEqual(os.path.join(cur_dir,'other-files','dummy3.h5'), actual_path)

    def test_appendFile_should_save_relative_path_when_possible(self):
        full_h5path = os.path.join(cur_dir,'files','dummy3.h5')
        shutil.copyfile(os.path.join(cur_dir,'other-files','dummy3.h5'), full_h5path)

        self.indexer.appendFile(full_h5path)

        conn = sqlite3.connect(dbPath, detect_types=sqlite3.PARSE_DECLTYPES)
        cur = conn.cursor()
        result = cur.execute('SELECT file FROM test WHERE uid=?',
                             ['cd3c070164561106e9b001888edc38fc']).fetchall()

        self.assertEqual("./dummy3.h5", result[0][0])

    def test_path_is_always_absolute_when_reading(self):
        full_h5path = os.path.join(cur_dir,'files','dummy3.h5')
        shutil.copyfile(os.path.join(cur_dir,'other-files','dummy3.h5'), full_h5path)

        self.indexer.appendFile(full_h5path)

        self.assertEqual(full_h5path,
                         self.indexer.searchUID('cd3c070164561106e9b001888edc38fc'))
        self.assertEqual((full_h5path,),
                         self.indexer.searchUID('cd3c070164561106e9b001888edc38fc', True))
        self.assertEqual(full_h5path,
                         self.indexer.list_tests()[-1][0])

    
    def test_change_comment(self):
        full_h5path = os.path.join(cur_dir,'files','hsm_test.h5')
        shutil.copyfile(real_test_file, full_h5path)

        self.indexer.appendFile(full_h5path)

        self.indexer.change_comment('a new comment',
                                    '5ed0a9b710d7f3030d0af3380e7129fe',
                                    full_h5path)

        hdf_file = SharedFile(full_h5path)
        hdf_file.set_version()
        saved_test_record = self.indexer.list_tests()[2]

        self.assertTrue('a new comment' in saved_test_record)
        self.assertEquals('a new comment', hdf_file.conf.hsm.measure['comment'])
    
    def test_change_name(self):
        full_h5path = os.path.join(cur_dir,'files','hsm_test.h5')
        shutil.copyfile(real_test_file, full_h5path)

        self.indexer.appendFile(full_h5path)

        self.indexer.change_name('a new name',
                                 '5ed0a9b710d7f3030d0af3380e7129fe',
                                 full_h5path)
        full_h5path = os.path.join(cur_dir,'files','a new name.h5')
        hdf_file = SharedFile(full_h5path)
        hdf_file.set_version()
        saved_test_record = self.indexer.list_tests()[2]

        self.assertTrue('a new name' in saved_test_record)
        self.assertEquals('a new name', hdf_file.conf.hsm.measure['name'])
        




if __name__ == "__main__":
    unittest.main()
