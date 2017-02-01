#!/usr/bin/python
# -*- coding: utf-8 -*-
import unittest
import shutil
import os
import tempfile

from misura.canon import indexer
from misura.canon.tests import testdir

print 'Importing', __name__

paths = [testdir + 'storage']
dbPath = testdir + 'storage/db'


class SharedFile(unittest.TestCase):
    path = testdir + 'storage/hsm_test.h5' 
    
    def setUp(self):
        self.test_file = tempfile.mktemp('.h5')
        shutil.copy(self.path, self.test_file)
        self.shared_file = indexer.SharedFile(self.test_file)

    def tearDown(self):
        self.shared_file.close()
        os.remove(self.test_file)


    def test_header(self):
        r = self.shared_file.header()
        self.assertTrue(len(r) > 1)

    def test_col(self):
        r = self.shared_file.col('/hsm/sample0/h', slice(0, 10))
        self.assertTrue(
            len(r) == 10, 'Query [10] returned wrong length [%i]' % len(r))

    def test_query_time(self):
        r = self.shared_file.col('/hsm/sample0/h', slice(0, 10))
        r1 = self.shared_file.query_time('/hsm/sample0/h', r[0][0], r[-1][0])
        r = r[:-1]  # escludo l'ultimo valore
        self.assertEqual(len(r), len(r1))
        self.assertEqual(r[0][0], r1[0][0])
        self.assertEqual(r[-1][0], r1[-1][0])
        # Stepping
        step = 0.3
        r2 = self.shared_file.query_time('/hsm/sample0/h', r[0][0], r[-1][0], step)
        self.assertLess(len(r2), len(r))
        self.assertLess(abs(r2[0][0] - r[0][0]), 0.3)
        self.assertLess(abs(r2[-1][0] - r[-1][0]), 0.3)

    def test_versions(self):
        shared_file = indexer.SharedFile(self.test_file)
        # Empty version
        self.assertEqual(shared_file.get_version(), '')
        self.assertEqual(shared_file.get_versions().keys(), [''])
        self.assertEqual(shared_file.get_versions().values()[0][0], 'Original')

        # Create new version
        shared_file.create_version('pippo')
        vd = shared_file.get_versions()
        self.assertEqual(shared_file.get_version(), '/ver_1')
        self.assertEqual(set(vd.keys()), set(['', '/ver_1']))
        self.assertEqual(
            set([info[0] for info in vd.values()]), set(['Original', 'pippo']))
        self.assertEqual(shared_file.test.root.conf.attrs.versions, 1)
        # Save a different /conf
        shared_file.load_conf()
        oname = shared_file.conf['name']
        nname = oname + '_pippo'
        shared_file.conf['name'] = nname
        shared_file.save_conf()
        # Force reload
        shared_file.load_conf()
        self.assertEqual(shared_file.conf['name'], nname)
        shared_file.set_version(0)
        self.assertEqual(shared_file.get_version(), '/ver_0')
        shared_file.set_version(1)
        # -1 should load lastly active version (0)
        shared_file.set_version(-1)
        self.assertEqual(shared_file.get_version(), '/ver_1')
        self.assertEqual(shared_file.conf['name'], nname)

    def test_should_keep_set_version(self):
        self.shared_file.create_version('a version')

        self.assertEqual(self.shared_file.get_versions()['/ver_1'][0], 'a version')
        #self.assertEqual(self.shared_file.get_version(), '/ver_1')

        self.shared_file.create_version('another version')
        self.assertEqual(self.shared_file.get_version(), '/ver_2')

        self.shared_file.set_version(1)
        self.assertEqual(self.shared_file.get_version(), '/ver_1')

        self.shared_file.set_version(2)
        self.assertEqual(self.shared_file.get_version(), '/ver_2')
        
    def test_plots(self):
        # Original version should have no plot functionality
        self.assertEqual(self.shared_file.get_plots(), {})
        self.assertFalse(self.shared_file.save_plot('fake plot text'))
        
        self.shared_file.create_version('plot0')
        self.assertEqual(self.shared_file.get_version(), '/ver_1')
        
        self.assertEqual((None, None), self.shared_file.get_plot('none'))
        format = 'jpg'
        render = 'fake jpg data'
        pid, title, date = self.shared_file.save_plot('fake plot text', title='A Fake Plot', 
                                                      render=render, render_format=format)
        self.assertEqual(pid, '0')
        self.assertEqual(title, 'A Fake Plot')
        text, attrs = self.shared_file.get_plot('0')
        self.assertEqual(text, 'fake plot text') 
        self.assertEqual(attrs['format'], 'jpg')
        self.assertEqual(attrs['title'], title)
        self.assertEqual(attrs['date'], date)
        
        plots = self.shared_file.get_plots(render=True)
        self.assertIn('0', plots)
        title1, date1, render1, format1 = plots['0']
        self.assertEqual(title1, title)
        self.assertEqual(date1, date)
        self.assertEqual(render1, 'fake jpg data')
        self.assertEqual(format1, format)
        
        
        

if __name__ == "__main__":
    unittest.main(verbosity=2)
