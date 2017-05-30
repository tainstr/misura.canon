#!/usr/bin/python
# -*- coding: utf-8 -*-
"""Test for Misura Language."""
import unittest
import numpy
from misura.canon import milang
from misura.canon import option
from misura.canon import logger
np = numpy


def setUpModule():
    print('Starting', __name__)


class DummyInterface(dict):
    dummy = {'Meta_sub': {
        'value': 1, 'point': 'None', 'time': 9, 'temp': 10}, 'fullpath': '/ciao/sub/'}
    devices = [dummy]
    log = logger.Log
    kiln = dummy
    outFile = None

    def __init__(self):
        dict.__init__(self, {'Meta_ciao': {
                      'value': 1, 'point': 'None', 'time': 9, 'temp': 10}, 'fullpath': '/ciao/', 'log':[0,'']})

    def child(self, name):
        r = self.get(name, None)
        if r is not None:
            return r
        return getattr(self, name, None)

    def log(self, *s):
        self['log'] = [0, ' '.join([str(e) for e in s])]


class InterfaceEnvironment(unittest.TestCase):
    obj = DummyInterface()
    ie = milang.InterfaceEnvironment()
    ie.obj = obj
    ie.hdf = obj

    env = milang.BaseEnvironment()

    def test_Log(self):
        self.ie.Log('hello', 1, None)
        self.assertEqual(self.ie.comment, 'hello 1 None')
        self.assertEqual(self.obj['log'][1], 'hello 1 None')

    def test_Opt(self):
        self.assertEqual(self.ie.Opt('Meta_ciao'), self.obj['Meta_ciao'])

    def test_Meta(self):
        self.assertEqual(self.ie.Meta('ciao'), self.obj['Meta_ciao'])

    def test_Metafunc(self):
        self.assertEqual(
            self.ie.MetaTime('ciao'), self.obj['Meta_ciao']['time'])
        self.assertEqual(
            self.ie.MetaTemp('ciao'), self.obj['Meta_ciao']['temp'])
        self.assertEqual(
            self.ie.MetaValue('ciao'), self.obj['Meta_ciao']['value'])

    def test_Leaf(self):
        self.assertEqual(
            self.ie.Leaf('dummy').Opt('Meta_sub'), self.obj.dummy['Meta_sub'])

    def test_compile(self):
        mi = milang.MiLang('mi.Value(obj.MetaTime("ciao"))', self.env, self.ie)
        mi.do()
        self.assertTrue(self.env.value, self.obj['Meta_ciao']['time'])

    def test_eval(self):
        mi = milang.MiLang(
            "L=obj.Leaf('dummy'); t=L.MetaTime('sub'); mi.Value(t)")
        mi.handle = 'ciao'
        out = DummyInterface()
        ins = DummyInterface()
        mi.eval(out, ins)
        self.assertEqual(mi.env.value, 9)


if __name__ == "__main__":
    unittest.main(verbosity=2)
