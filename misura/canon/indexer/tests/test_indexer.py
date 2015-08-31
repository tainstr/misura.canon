#!/usr/bin/python
# -*- coding: utf-8 -*-
import unittest
from misura import parameters as params
from misura.canon import indexer

import os

cur_dir = os.path.dirname(os.path.realpath(__file__))

paths = [cur_dir + '/files']
dbPath = cur_dir + '/files/test.sqlite'


class Indexer(unittest.TestCase):

    def setUp(self):
        self.indexer = indexer.Indexer(paths=paths)
        self.indexer.open_db(dbPath)
        self.indexer.close_db()

    def test_rebuild(self):
        self.indexer.rebuild()
        self.assertEqual(2, self.indexer.get_len())

    def test_header(self):
        header = self.indexer.header()
        self.assertEqual(['file', 'serial', 'uid', 'id', 'date', 'instrument',
                          'flavour', 'name', 'elapsed', 'nSamples', 'comment', 'verify'], header)

    def test_query(self):
        result = self.indexer.query()
        instrument = result[0][5]

        result = self.indexer.query({'instrument': instrument})
        self.assertEqual(len(result), 1)

        result = self.indexer.query({'instrument': 'pippo'})
        self.assertEqual(len(result), 0)

    def test_searchUID(self):
        result = self.indexer.searchUID('eadd3abc68fa78ad64eb6df7174237a0')
        self.assertEqual(result, cur_dir + '/files/dummy1.h5')

if __name__ == "__main__":
    unittest.main()
