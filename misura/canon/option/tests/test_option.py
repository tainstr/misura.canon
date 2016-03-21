#!/usr/bin/python
# -*- coding: utf-8 -*-
import unittest
from misura.canon import option
from misura.canon.tests import testdir

c1 = testdir + 'storage/Conf.csv'
c2 = testdir + 'storage/Conf2.csv'
tmp = testdir + 'storage/tmpfile'
db = testdir + 'storage/tmpdb'



print 'Importing test_option'


def setUpModule():
    print 'Starting test_option'


class Option(unittest.TestCase):

    """Tests the basic option.Option object"""

    def test_option(self):
        o = option.Option(current=0, handle='test', type='Integer')
        self.assertEqual(o.get(), 0)
        self.assertEqual(o.get(), o['current'])
        self.assertEqual(o.get(), o.get('current'))
        o['csunit'] = 'minute'
        self.assertEqual(o.get(), 0)
        self.assertEqual(o['csunit'], 'minute')

    def test_migrate(self):
        old = option.Option(current=0, handle='test', type='Integer')
        old.validate()
        new = option.Option(current='1', handle='test', type='String')
        new.validate()
        new.migrate_from(old)
        new.validate()
        # Should retain type
        self.assertEqual(new['type'], 'Integer')
        # But should try to update value by converting it
        self.assertEqual(new['current'], 1)

        # Fail conversion
        new = option.Option(current='fail', handle='test', type='String')
        new.validate()
        new.migrate_from(old)
        new.validate()
        # Should retain type
        self.assertEqual(new['type'], 'Integer')
        # But as it cannot be converted, should keep old current value
        self.assertEqual(new['current'], 0)
        
        # Table conversion
        oldh =[('A','A'),('B','B')]
        old = option.Option(current=[oldh,[1,1]],handle='test',type='Table')
        old.validate()
        new = option.Option(current=[oldh,[2,2]],handle='test',type='Table')
        new.validate()
        new.migrate_from(old)
        new.validate()
        self.assertEqual(new['current'][1], [2,2])
        newh = [('C','A'),('D','B')]
        new['current'] = [newh,[2,2]]
        new.migrate_from(old)
        new.validate()
        self.assertEqual(new['current'], [newh,[2,2]])
        newh = [('A','A'),('B','E')]
        new['current'] = [newh,[2,2]]
        new.migrate_from(old)
        new.validate()
        self.assertEqual(new['current'], [newh])
        
        


if __name__ == "__main__":
    unittest.main()
