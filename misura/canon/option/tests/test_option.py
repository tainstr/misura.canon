#!/usr/bin/python
# -*- coding: utf-8 -*-
import unittest
from misura import parameters as params
from misura.canon import option


c1 = params.testdir + 'storage/Conf.csv'
c2 = params.testdir + 'storage/Conf2.csv'
tmp = params.testdir + 'storage/tmpfile'
db = params.testdir + 'storage/tmpdb'

c3 = params.mdir + 'conf/MeasureFlex.csv'
c4 = params.mdir + 'conf/Standard.csv'

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


if __name__ == "__main__":
    unittest.main()
