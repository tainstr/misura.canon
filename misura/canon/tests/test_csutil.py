#!/usr/bin/python
# -*- coding: utf-8 -*-
import unittest
import tempfile
from misura.canon.csutil import next_point
from misura.canon.csutil import filter_calibration_filenames
from misura.canon.csutil import only_hdf_files
from misura.canon.csutil import incremental_filename


class TestCsUtil(unittest.TestCase):

    def test_next_point_only_strings(self):
        curve = [[1, "curve", 2, 3], [3, "with", 4, 5],
                 [5, "only", 6, 7], [7, "strings", 8, 9]]

        actual_row_index, next_row = next_point(curve, 2)

        self.assertEqual(actual_row_index, len(curve))
        self.assertFalse(next_row)

    def test_next_point_negative_row_index(self):
        curve = ["any", "curve", 3, 4]

        actual_row_index, next_row = next_point(curve, -10)

        self.assertEqual(actual_row_index, -1)
        self.assertFalse(next_row)

    def test_next_point(self):
        curve = [['any value', 2, 'any value', 'any value'],
                 ['any value', 'a string', 'any value', 'any value'],
                 ['any value', 456, 'any value', 'any value']]

        actual_row_index, next_row = next_point(curve, 1)

        self.assertEqual(actual_row_index, 2)
        self.assertEqual(
            ['any value', 456, 'any value', 'any value'], next_row)

    def test_filter_calibration_filenames(self):
        filenames = ['a file name',
                      'another filename',
                      'a /calibRation/ filename',
                      'yet another filename']

        expected_filenames = ['a file name',
                              'another filename',
                              'yet another filename']

        self.assertEqual(expected_filenames, filter_calibration_filenames(filenames))

    def test_only_hdf_files(self):
        filenames = ['a not hdf file',
                     'an hdf file.h5',
                     'another file',
                     'another hdf file.h5',
                     'yet another file']

        filtered_filenames = ['an hdf file.h5',
                              'another hdf file.h5']

        self.assertEqual(filtered_filenames, only_hdf_files(filenames))
        
    def test_incremental_filename(self):
        self.assertEqual(incremental_filename(''), '')
        self.assertEqual(incremental_filename('asdgs'), 'asdgs')
        self.assertEqual(incremental_filename('asdgs.exe'), 'asdgs.exe')
        self.assertEqual(incremental_filename('a_b_1.g'), 'a_b_1.g')
        with tempfile.NamedTemporaryFile() as f:
            self.assertEqual(incremental_filename(f.name), f.name+'_0')
        with tempfile.NamedTemporaryFile(suffix='_12') as f:
            self.assertEqual(incremental_filename(f.name), f.name[:-3]+'_13')
        with tempfile.NamedTemporaryFile(suffix='.exe') as f:
            self.assertEqual(incremental_filename(f.name), f.name[:-4]+'_0.exe')
        with tempfile.NamedTemporaryFile(suffix='_15.f') as f:
            self.assertEqual(incremental_filename(f.name), f.name[:-5]+'_16.f')          
        with tempfile.NamedTemporaryFile(suffix='_1_2_dsf_1_56_d.f') as f:
            self.assertEqual(incremental_filename(f.name), f.name[:-2]+'_0.f')

if __name__ == "__main__":
    unittest.main(verbosity=2)
