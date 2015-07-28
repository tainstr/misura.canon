#!/usr/bin/python
# -*- coding: utf-8 -*-
import unittest
from misura import parameters as params
from misura.canon import indexer
import shutil
import os
import tempfile
import tables
print 'Importing', __name__

paths = [params.testdir + 'storage']
dbPath = params.testdir + 'storage/db'


def setUpModule():
    print 'Starting', __name__, tables.__version__


class SharedFile(unittest.TestCase):
    #	path=params.testdir+'storage/data/flex/default'+params.ext
    path = params.testdir + 'storage/hsm_test' + params.ext

    @classmethod
    def setUpClass(c):
        c.path = params.testdir + 'storage/hsm_test' + params.ext
        c.store = indexer.SharedFile(c.path)

    def test_header(self):
        r = self.store.header()
        self.assertTrue(len(r) > 1)

    def test_col(self):
        r = self.store.col('/hsm/sample0/h', slice(0, 10))
        self.assertTrue(
            len(r) == 10, 'Query [10] returned wrong length [%i]' % len(r))

    def test_query_time(self):
        r = self.store.col('/hsm/sample0/h', slice(0, 10))
        r1 = self.store.query_time('/hsm/sample0/h', r[0][0], r[-1][0])
        r = r[:-1]  # escludo l'ultimo valore
        self.assertEqual(len(r), len(r1))
        self.assertEqual(r[0][0], r1[0][0])
        self.assertEqual(r[-1][0], r1[-1][0])
        # Stepping
        step = 0.3
        r2 = self.store.query_time('/hsm/sample0/h', r[0][0], r[-1][0], step)
        self.assertLess(len(r2), len(r))
        self.assertLess(abs(r2[0][0] - r[0][0]), 0.3)
        self.assertLess(abs(r2[-1][0] - r[-1][0]), 0.3)

    def test_versions(self):
        fn = tempfile.mktemp('.h5')
        shutil.copy(self.path, fn)
        print 'opening temporary file', fn
        f = indexer.SharedFile(fn)
        # Empty version
        self.assertEqual(f.get_version(), '')
        self.assertEqual(f.get_versions().keys(), [''])
        self.assertEqual(f.get_versions().values()[0][0], 'Original')

        # Create new version
        f.create_version('pippo')
        vd = f.get_versions()
        self.assertEqual(f.get_version(), '/ver_1')
        self.assertEqual(set(vd.keys()), set(['', '/ver_1']))
        self.assertEqual(
            set([info[0] for info in vd.values()]), set(['Original', 'pippo']))
        self.assertEqual(f.test.root.conf.attrs.versions, 1)
        # Save a different /conf
        f.load_conf()
        oname = f.conf['name']
        nname = oname + '_pippo'
        f.conf['name'] = nname
        f.save_conf()
        # Force reload
        f.load_conf()
        self.assertEqual(f.conf['name'], nname)
        f.set_version(0)
        self.assertEqual(f.conf['name'], oname)
        # -1 should load latest version
        f.set_version(-1)
        self.assertEqual(f.get_version(), '/ver_1')
        self.assertEqual(f.conf['name'], nname)
        f.close()
        os.remove(fn)

    @classmethod
    def tearDownClass(c):
        c.store.close()

if __name__ == "__main__":
    unittest.main(verbosity=2)
