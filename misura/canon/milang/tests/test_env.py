#!/usr/bin/python
# -*- coding: utf-8 -*-
"""Test per Mis
ura Language."""
import unittest
import numpy
from misura.canon import milang
np = numpy


def setUpModule():
    print 'Starting', __name__


class BaseEnvironment(unittest.TestCase):
    env = milang.BaseEnvironment()

    def test_Value(self):
        self.env.value = 1
        mi = milang.MiLang('mi.Value(10)', env=self.env)
        mi.do()
        self.assertEqual(mi.env.value, 10)
        self.assertEqual(self.env.time, None)

    def test_Where(self):
        n = np.random.random(100)
        w0 = self.env.Where(n > 0.8)
        w1 = np.where(n > 0.8)[0][0]
        print w0, w1
        self.assertEqual(w0, w1)


if __name__ == "__main__":
    unittest.main(verbosity=2)
