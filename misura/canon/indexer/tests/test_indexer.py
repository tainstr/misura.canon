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
        self.indexer = indexer.Indexer(paths = paths)
        self.indexer.open_db(dbPath)
        self.indexer.close_db()

    def test_rebuild(self):
        self.indexer.rebuild()
        self.assertEqual(2, self.indexer.get_len())

    # def test_1_header(self):
    #     h = self.store.header()

    # def test_2_listMaterials(self):
    #     self.store.listMaterials()

    # def test_3_query(self):
    #     r = self.store.query()
    #     instr = r[0][5]
    #     n = 0
    #     for e in r:
    #         if e[5] == instr:
    #             n += 1
    #     r = self.store.query({'instrument': instr})
    #     self.assertEqual(len(r), n)
    #     r = self.store.query({'instrument': 'pippo'})
    #     self.assertEqual(len(r), 0)

    # def test_4_searchUID(self):
    #     r = self.store.query()
    #     path = r[0][0]
    #     uid = r[0][2]
    #     r = self.store.searchUID(uid)
    #     self.assertEqual(r, path)

if __name__ == "__main__":
    unittest.main()
