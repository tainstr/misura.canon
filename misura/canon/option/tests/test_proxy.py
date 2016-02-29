#!/usr/bin/python
# -*- coding: utf-8 -*-
import unittest
from misura import parameters as params
from misura.canon import indexer
from misura.canon import dataimport

path = params.testdir + 'storage/hsm_test' + params.ext
# path='/opt/shared_data/hsm/test_polvere_18.h5'


class ConfigurationProxy(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        sh = indexer.SharedFile(path)
        sh.load_conf()
        cls.shared_file = sh
        
    @classmethod
    def tearDownClass(cls):
        cls.shared_file.close()
    
    def test_run_scripts(self):
        self.shared_file.run_scripts(self.shared_file.conf.hsm)
        
    def test_add_option(self):
        sh = self.shared_file
        sh.conf.kiln.add_option('testOpt','Boolean',True,'Test option')
        self.  assertTrue(sh.conf.kiln['testOpt'])
        
    def test_add_child(self):
        base = dataimport.base_dict()
        base['name']['current'] = 'pippo'
        added = self.shared_file.conf.kiln.add_child('new',base)
        self.assertEqual(added['name'], 'pippo')
        self.assertEqual(self.shared_file.conf.kiln.new['name'], 'pippo')
    


if __name__ == "__main__":
    unittest.main(verbosity=2)
