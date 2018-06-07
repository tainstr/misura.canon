#!/usr/bin/python
# -*- coding: utf-8 -*-
import unittest
from misura.canon import indexer, option
from misura.canon.plugin import dataimport
from misura.canon.tests import testdir

path = testdir + 'storage/hsm_test.h5'
# path='/opt/shared_data/hsm/test_polvere_18.h5'
_calls = []


def dummy_callback(conf, key, old_val, new_val):
    d = conf.gete(key)
    if not d.get('callback_set', False) == 'dummy':
        return new_val
    print('Dummy callback', conf['fullpath'], key, old_val, new_val)
    _calls.append((conf['fullpath'], key, old_val, new_val))
    return new_val


option.proxy.ConfigurationProxy.callbacks_set.add(dummy_callback)

def add_target(base, name, val):
    child = option.ConfigurationProxy({'self': dataimport.base_dict()})
    child.add_option('a', 'Float', val, 'Col A')
    child.add_option('b', 'Float', val * 2, 'Col B')
    child.add_option(
        'sum', 'Float', -1, 'Aggregate sum', aggregate='sum(a)')
    child.add_option(
        'mean', 'Float', -1, 'Aggregate mean', aggregate='mean(a)', error='meanError')
    child.add_option('meanError', 'Float', -1, 'Mean result error',)
    child.add_option(
        'prod', 'Float', -1, 'Aggregate prod', aggregate='prod(a)')
    child.add_option('table', 'Table',
                    [[('Bla Bla', 'Float'), ('Bla Bla Bla', 'Float')],
                     [7, 8]], 'Aggregate result',
                    aggregate='table(a,b)')
    child.add_option('table_flat', 'Table', [],'Aggregate table flat',
                    aggregate='table_flat(a,b)')
    child.add_option('table_flat_aggr', 'Table', [], 'Aggregate table flat from aggregates',
                    aggregate='table_flat(sum,mean)')
    base.add_child(name, child.describe())

def create_aggregate():
    base = option.ConfigurationProxy({'self': dataimport.base_dict()})
    base.add_option(
        'a', 'Float', -1, 'Aggregate result', aggregate='sum()')
    base.add_option(
        'sum', 'Float', -1, 'Aggregate sum', aggregate='sum(a)')
    base.add_option(
        'mean', 'Float', -1, 'Aggregate mean', aggregate='mean(a)', error='meanError')
    base.add_option('meanError', 'Float', -1, 'Mean result error',)
    base.add_option(
        'prod', 'Float', -1, 'Aggregate prod', aggregate='prod(a)')
    
    base.add_option('table', 'Table',
                    [[('Bla Bla', 'Float'), ('Bla Bla Bla', 'Float')],
                     [7, 8]], 'Aggregate table',
                    aggregate='table(a,b)')
    base.add_option('table_flat', 'Table', [], 'Aggregate table flat',
                    aggregate='table_flat(a,b)')
    base.add_option('table_flat_aggr', 'Table', [], 'Aggregate table flat from aggregates',
                    aggregate='table_flat(sum,mean)')

    add_target(base, 'ch1', 1)
    add_target(base, 'ch2', 2)
    add_target(base, 'ch3', 6)
    
    add_target(base.child('ch1'), 'sch11', 10)
    add_target(base.child('ch1'), 'sch12', 20)
    add_target(base.child('ch1'), 'sch13', 60)
     
    add_target(base.child('ch2'), 'sch21', 100)
    add_target(base.child('ch2'), 'sch22', 200)
    add_target(base.child('ch2'), 'sch23', 600)
     
    add_target(base.child('ch3'), 'sch31', 0.1)
    add_target(base.child('ch3'), 'sch32', 0.2)
    add_target(base.child('ch3'), 'sch33', 0.6)
    return base
    

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
        r = self.shared_file.run_scripts(self.shared_file.conf.hsm)
        self.assertTrue(r)

    def test_add_option(self):
        sh = self.shared_file
        sh.conf.kiln.add_option('testOpt', 'Boolean', True, 'Test option')
        self.assertTrue(sh.conf.kiln['testOpt'])

    def test_add_child(self):
        base = dataimport.base_dict()
        base['name']['current'] = 'pippo'
        added = self.shared_file.conf.kiln.add_child('new', base)
        self.assertEqual(added['name'], 'pippo')
        self.assertEqual(self.shared_file.conf.kiln.new['name'], 'pippo')
        t = self.shared_file.conf.tree()
        self.assertIn('new', t['kiln'])
        self.assertEqual(t['kiln']['new']['self']['name']['current'], 'pippo')
        rm = self.shared_file.conf.rmodel()
        self.assertIn('new', rm['kiln'])

    def test_autosort(self):
        def ac(name):
            base = dataimport.base_dict()
            base['name']['current'] = name
            added = self.shared_file.conf.kiln.add_child(name, base)
        ac('T10')
        ac('T100')
        ac('T30')
        ac('T20')
        ac('T1000')
        ac('T200')
        self.assertEqual(set(self.shared_file.conf.kiln.children.keys()), set(['sample0',
                                                                               'T10', 'T20', 'T30', 'T100', 'T200', 'T1000',
                                                                               'heatload', 'measure', 'new', 'regulator']))


    
    def test_calc_aggregate(self):
        base = create_aggregate()
        aval, error, tree = base.calc_aggregate('sum(a)')
        self.assertEqual(aval, 9)
        self.assertEqual(error, None)
        oktree = [[1, 'ch1'], [2, 'ch2'], [6, 'ch3']]
        self.assertEqual(tree, oktree)
        aval, error, tree = base.calc_aggregate('mean(a)')
        self.assertEqual(aval, 3)
        self.assertAlmostEqual(error, 2.1602468)
        self.assertEqual(tree, oktree)
        aval, error, tree = base.calc_aggregate('prod(a)')
        self.assertEqual(aval, 12)
        self.assertEqual(tree, oktree)
        self.assertEqual(error, None)
        aval, error, tree = base.calc_aggregate('table(a,b)', 'table')
        self.assertEqual(
            aval, [[('Col A', 'Float'), ('Col B', 'Float')], [1, 2], [2, 4], [6, 12]])
        self.assertEqual(tree, [[1, 2, 'ch1'], [2, 4, 'ch2'], [6, 12, 'ch3']])
        aval2, error, tree = base.calc_aggregate('table( a , b)', 'table')
        self.assertEqual(aval, aval2)
        
        aval, error, tree = base.calc_aggregate('table_flat(a,b)', 'table')
        self.assertEqual(
            aval, [[('Col A', 'Float'),
   ('ch1 Col A', 'Float'),
   ('ch2 Col A', 'Float'),
   ('ch3 Col A', 'Float'),
   ('Col B', 'Float'),
   ('ch1 Col B', 'Float'),
   ('ch2 Col B', 'Float'),
  ('ch3 Col B', 'Float')],
  [1, 1, 2, 6, 2, 2, 4, 12],
  [2, 1, 2, 6, 4, 2, 4, 12],
  [6, 1, 2, 6, 12, 2, 4, 12]])
        
        aval, error, tree = base.calc_aggregate('makegold(a)')
        self.assertEqual(aval, None)

        base.update_aggregates()
        self.assertEqual(base['a'], 9)
        self.assertEqual(base['sum'], 9)
        self.assertEqual(base.getattr('sum', 'tree'), oktree)
        self.assertEqual(base['mean'], 3)
        self.assertEqual(base.getattr('mean', 'tree'), oktree)
        self.assertAlmostEqual(base['meanError'], 2.1602468)
        self.assertEqual(base['prod'], 12)
        self.assertEqual(base.getattr('prod', 'tree'), oktree)
        self.assertEqual(base['table'], [
                         [('Col A', 'Float'), ('Col B', 'Float')], [1, 2], [2, 4], [6, 12]])
        self.assertEqual(base.getattr('table', 'tree'),
                        [[1, 2, 'ch1'], [2, 4, 'ch2'], [6, 12, 'ch3']])
        
    def test_table_flat(self):
        base = create_aggregate()
        base.update_aggregates(recursive=-1)
        ch1 = base.child('ch1') 
        sch11 = ch1.child('sch11')
        sch11.update_aggregates()
        # A leaf cannot aggregate
        self.assertEqual(sch11['table'],[[('Bla Bla', 'Float'), ('Bla Bla Bla', 'Float')], [7, 8]])
        self.assertEqual(sch11['table_flat'],[])
        self.assertEqual(sch11['table_flat_aggr'],[])
        
        ###
        # Second level
        ###
        self.assertEqual(ch1['table'], [[('Col A', 'Float'), ('Col B', 'Float')], [10, 20], [20, 40], [60, 120]])
        # Not involving sub-aggregates
        self.assertEqual(ch1['table_flat'], [[('Col A', 'Float'), ('Col B', 'Float')], [10, 20], [20, 40], [60, 120]])
        
        print 'TREE',ch1.gete('table_flat_aggr')['tree'][1]
        print 'TABLE header', ch1['table_flat_aggr'][0]
        print 'TABLE row[0]', ch1['table_flat_aggr'][1]        
          
        # Only contains empty sub-aggregates
        self.assertEqual(ch1['table_flat_aggr'], [[('Aggregate sum', 'Float'), ('Aggregate mean', 'Float')],
                                                    [-1, -1],
                                                    [-1, -1],
                                                    [-1, -1]])
        
        ###        
        # First level
        ###
        self.assertEqual(base['table'], [[('Col A', 'Float'), ('Col B', 'Float')], [1, 2], [2, 4], [6, 12]])
        # Not involving sub-aggregates, it's equal to table():
        self.assertEqual(base['table_flat'],[[('Col A', 'Float'), ('Col B', 'Float')], [1, 2], [2, 4], [6, 12]])
        
        
        
        print 'TREE',base.gete('table_flat_aggr')['tree'][1]
        print 'TABLE header', base['table_flat_aggr'][0]
        print 'TABLE row[0]', base['table_flat_aggr'][1]
        for i,h in enumerate(base['table_flat_aggr'][0]):
            print h, [row[i] for row in base['table_flat_aggr'][1:]]
        self.assertEqual(base['table_flat_aggr'], [[('Aggregate sum', 'Float'), 
                                                   ('sch11 Aggregate sum', 'Float'), 
                                                   ('sch12 Aggregate sum', 'Float'), 
                                                   ('sch13 Aggregate sum', 'Float'), 
                                                   ('Aggregate mean', 'Float'), 
                                                   ('sch11 Aggregate mean', 'Float'), 
                                                   ('sch12 Aggregate mean', 'Float'), 
                                                   ('sch13 Aggregate mean', 'Float')],
                                                    [0.9000000357627869, 0.1, 0.2, 0.6, 0.30000001192092896, 0.1, 0.2, 0.6],
                                                    [90.0, 10, 20, 60, 30.0, 10, 20, 60],
                                                    [900.0, 100, 200, 600, 300.0, 100, 200, 600]])
        

    def test_aggregate_merge_tables(self):
        a = option.ConfigurationProxy({'self': dataimport.base_dict()})
        a.add_option('a', 'Table',
                     [[('Key', 'Float'), ('A', 'Float')],
                         [1, 10], [2, 20], [3, 30], [4, 40], [5, 50]], 'Tab A',
                     unit=['meter', 'second'], precision=[1, 2])
        a['name'] = 'Dev A'
        b = option.ConfigurationProxy({'self': dataimport.base_dict()})
        b.add_option('b', 'Table',
                     [[('Key', 'Float'), ('B', 'Float')],
                         [1, 0.1], [2, 0.2], [3, 0.3], [4, 0.4], [5, 0.5]], 'Tab B',
                     unit=['meter', 'celsius'], precision=[1, 3])
        b['name'] = 'Dev B'
        c = option.ConfigurationProxy({'self': dataimport.base_dict()})
        c.add_option('c', 'Table',
                     [[('Key', 'Float'), ('C', 'Float')],
                         [1, 100], [2, 200], [3, 300], [4, 400], [5, 500]], 'Tab C',
                     unit=['meter', 'volt'], precision=[1, 4])
        c['name'] = 'Dev C'

        targets = ['a', 'b', 'c']
        devices = {'a': [a], 'b': [b], 'c': [c]}

        values = {name: [dev[0][name]] for name, dev in devices.items()}

        result, unit, precision, visible = option.aggregate_merge_tables(
            targets, values, devices)
        self.assertEqual(len(result[0]), 4)
        self.assertEqual(result[0], [('Key', 'Float'), ('Dev A\nA', 'Float'),
                                     ('Dev B\nB', 'Float'), ('Dev C\nC', 'Float')])
        self.assertEqual(len(result[1]), 4)
        self.assertEqual(result[5], [5, 50, 0.5, 500])
        self.assertEqual(unit, ['meter', 'second', 'celsius', 'volt'])
        self.assertEqual(precision, [1, 2, 3, 4])

    def test_callback(self):
        base = option.ConfigurationProxy({'self': dataimport.base_dict()})
        base.setattr('name', 'callback_set', 'dummy')
        self.assertEqual(len(_calls), 0)
        base['name'] = 'ciao'
        self.assertEqual(len(_calls), 1)
        self.assertEqual(_calls[-1][1], 'name')


if __name__ == "__main__":
    unittest.main(verbosity=2)
