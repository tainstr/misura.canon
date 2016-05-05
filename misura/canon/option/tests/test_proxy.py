#!/usr/bin/python
# -*- coding: utf-8 -*-
import unittest
from misura.canon import indexer, option
from misura.canon.plugin import dataimport
from misura.canon.tests import testdir

path = testdir + 'storage/hsm_test.h5'
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
        
    def test_calc_aggregate(self):
        base = option.ConfigurationProxy({'self':dataimport.base_dict()})
        base.add_option('a','Float',-1,'Aggregate result', aggregate='sum()')
        base.add_option('sum','Float',-1,'Aggregate result', aggregate='sum(a)')
        base.add_option('mean','Float',-1,'Aggregate result', aggregate='mean(a)')
        base.add_option('prod','Float',-1,'Aggregate result', aggregate='prod(a)')
        base.add_option('table','Table',
                        [[('Col A', 'Float'), ('Col B', 'Float')], [7,8]],'Aggregate result', 
                        aggregate='table(a,b)')
        def add_target(name, val):
            child = option.ConfigurationProxy({'self':dataimport.base_dict()})
            child.add_option('a','Float',val,'Aggregate target')
            child.add_option('b','Float',val*2,'Aggregate target col')
            base.add_child(name,child.describe())
        add_target('ch1', 1)
        add_target('ch2', 2)
        add_target('ch3', 6)
        aval = base.calc_aggregate('sum(a)')
        self.assertEqual(aval, 9)
        aval = base.calc_aggregate('mean(a)')
        self.assertEqual(aval, 3)
        aval = base.calc_aggregate('prod(a)')
        self.assertEqual(aval, 12)
        aval = base.calc_aggregate('table(a,b)', 'table')
        self.assertEqual(aval, [[('Col A', 'Float'), ('Col B', 'Float')], [1,2],[2,4],[6,12]])
        aval2 = base.calc_aggregate('table( a, b)', 'table')
        self.assertEqual(aval, aval2)
        aval = base.calc_aggregate('makegold(a)')
        self.assertEqual(aval, None)
        
        base.update_aggregates()
        self.assertEqual(base['a'], 9)
        self.assertEqual(base['sum'], 9)
        self.assertEqual(base['mean'], 3)
        self.assertEqual(base['prod'], 12)
        self.assertEqual(base['table'], [[('Col A', 'Float'), ('Col B', 'Float')], [1,2],[2,4],[6,12]])
        
        
         
    


if __name__ == "__main__":
    unittest.main(verbosity=2)
