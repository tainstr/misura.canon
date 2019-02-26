#!/usr/bin/python
# -*- coding: utf-8 -*-
import unittest
import os
from misura.canon.tests import testdir
from misura.canon import indexer
print('Importing test_indexer')

paths = [testdir + 'storage']
dbPath = testdir + 'storage/db'


def setUpModule():
    print('Starting', __name__)


class FileManager(unittest.TestCase):
    uid = '5ed0a9b710d7f3030d0af3380e7129fe'

    @classmethod
    def setUpClass(c):
        os.remove(dbPath)
        store = indexer.Indexer(dbPath, paths)
        print('FileManager', dbPath, paths, store.rebuild())
        c.m = indexer.FileManager(store)

    def test_0_open_uid(self):
        r = self.m.open_uid(self.uid)
        self.assertTrue(r)
        print(r)

    def test_1_uid(self):
        s = self.m.uid(self.uid)
        self.assertTrue(s)

    @classmethod
    def tearDownClass(c):
        s = c.m.uid(c.uid)
        if s:
            s.close()
        c.m.close()

if __name__ == "__main__":
    unittest.main()
