#!/usr/bin/python
# -*- coding: utf-8 -*-
"""Test per Mis
ura Language."""
import unittest
import numpy
from misura.canon import milang, indexer

from misura.canon.tests import testdir, DummyInstrument, verify_point

np = numpy


def setUpModule():
    print 'Starting', __name__


data_scr = """
print 'data_scr'
i0,t0,v0=mi.Raises('cohe',80)
print 'raises',i0,t0,v0
mi.t(t0)
T=mi.At('T',t0)
print 'Set T',T
mi.T(T)
mi.t(t0)
# Functions should work also directly on array objects
w=mi.Curve('w')
mi.Value(mi.At(w,t0))
mi.Log("Ciao")
# Multiple assign
x,y=mi.xy('w')
"""


cooling0_scr = """
print 'Getting Max'
maxT=mi.Max('T')
print 'Equals'
imaxT=mi.Equals('T',maxT)
i=mi.Drops('T',78, imaxT)
mi.t(i)
"""

# FIXME: for this to work, we need real data here!
path = testdir + 'storage/hsm_test.h5'


class DataEnvironment(unittest.TestCase):
    env = milang.DataEnvironment()
# 	tab=ut.FakeStorageFile()
    tab = indexer.SharedFile(path)
    env.hdf = tab
    env.prefix = '/hsm/sample0/'

    def test_Value(self):
        self.env.Value(10)
        self.assertEqual(self.env.value, 10)

    def test_simple(self):
        mi = milang.MiLang(data_scr, env=self.env)
        self.assertTrue(mi.code)
        mi.ins_env.obj = DummyInstrument()
        mi.do()
        print 'verify point', mi.env.time, mi.env.temp, mi.env.value
        verify_point(self, mi.env, 1402241667.782006,
                        20.01248106656483,  215.33565530223945, 'Ciao')

    def test_Select(self):
        mi = milang.MiLang(cooling0_scr, env=self.env)
        self.assertTrue(mi.code)
        mi.ins_env.obj = DummyInstrument()
        mi.do()


if __name__ == "__main__":
    unittest.main(verbosity=2)
