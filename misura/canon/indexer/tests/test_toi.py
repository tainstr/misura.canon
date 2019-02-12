# -*- coding: utf-8 -*-
"""Tests for Total Option Indexing"""

import unittest
import sqlite3
from misura.canon import indexer, option
from misura.canon.tests import testdir
from misura.canon.indexer.interface import SharedFile
from misura.canon.indexer import toi
from misura.canon.plugin import dataimport
import os
import shutil
import sqlite3



cur_dir = os.path.dirname(os.path.realpath(__file__))
real_test_file = testdir + 'storage/hsm_test.h5'
testdb = testdir + 'storage/toi.sqlite'
#real_test_file = '/home/daniele/MisuraData/toi/test.h5' 
#testdb = '/home/daniele/MisuraData/toi/toi.sqlite'




class TestTotalOptionIndexer(unittest.TestCase):

    def setUp(self):
        if os.path.exists(testdb):
            os.remove(testdb)
        self.conn = sqlite3.connect(testdb, detect_types=sqlite3.PARSE_DECLTYPES)
        self.cur = self.conn.cursor()
        toi.create_tables(self.cur)
        toi.create_views(self.cur)
        self.conn.commit()
        
    
    def tearDown(self):
        self.conn.close()
        #if os.path.exists(testdb)
        #    os.remove(testdb)
        

    def test_create_tables(self):    
        self.assertTrue(os.path.exists(testdb))
        #TODO: check if all tables are defined
        
    def test_index_option(self):
        opt = option.ao([],'test0','String', 'bla bla')[0]
        toi.index_option(self.cur, 'abcde','ver_1', '/option/full/path/', opt)
        self.conn.commit()
        
    def test_index_desc(self):
        desc = {}
        desc['self'] = 'should_not_try_me'
        option.ao(desc,'fullpath','String', '/option/full/path2/')
        option.ao(desc,'test','String', 'bla bla')
        option.ao(desc,'test2','Float', 2.13)
        option.ao(desc,'test3','Integer', 3)
        option.ao(desc,'test4','Boolean', True)
        option.ao(desc,'test5','Boolean', False)
        option.ao(desc,'test6','Meta', {'temp':861.6, 'time': 1291, 'value': 0.091})
        option.ao(desc,'test7','String', 'Seven') 
        toi.index_desc(self.cur, 'abcde', 'ver_2', desc)  
        self.conn.commit()                             
        
    def test_index_tree(self):
        tree = dataimport.tree_dict()
        toi.index_tree(self.cur, 'xyz', 'pippo', tree)
        self.conn.commit()
        
    def test_index_plots(self):
        sh = SharedFile(real_test_file, mode='r')
        r = toi.index_plots(self.cur, sh, 'ver_1')
        self.conn.commit()
        sh.close()
        self.assertEqual(r,0)
        
    
    def test_index_version(self):
        sh = SharedFile(real_test_file, mode='r')
        r = toi.index_version(self.cur, sh, 'ver_1', 'TestVersion', '12/4/2018', True)
        sh.close()
        self.conn.commit()
        self.assertTrue(r)
        

    def test_index_file(self):
        sh = SharedFile(real_test_file, mode='r')
        r = toi.index_file(self.cur, sh)
        sh.close()
        self.conn.commit()
        self.assertTrue(r)
        


        





        

if __name__ == "__main__":
    unittest.main(verbosity=2)