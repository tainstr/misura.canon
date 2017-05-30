#!/usr/bin/python
# -*- coding: utf-8 -*-
"""Test per Mis
ura Language."""
import unittest
import numpy
from misura.canon import milang
np = numpy


def setUpModule():
    print('Starting', __name__)

# Simple scripts for validation

ok_scr = """def f(): return 1
a=f()
b=(a+2)*3
d=b+1
"""

err_scr = ok_scr + """
e=forbid(d)
import os
os.open('')"""

exe_scr = """def f(): return 1
a=f()
b=(a+2)*3
d=b+2
d=mi.dummy(d,5)
mi.Value(d**2)
"""

exp1_scr = """
def a(): pass
def read(): pass
a=open
v=a('/etc/issue','r').read()
print(v)
mi.Value(v)
"""

exp2_scr = "mi.At=open; v=mi.At('/etc/issue','r'); print(v); mi.Value(v)"

exp3_scr = "mi.At,foo=(open,1); v=mi.At('/etc/issue','r')"

exploits = [(exp1_scr, 'By new definition'),
            (exp2_scr, 'By redefinition'),
            (exp3_scr, 'By list assignment redefinition')]


class Validator(unittest.TestCase):

    """Code validation and secure execution"""

    def test_failing(self):
        mi = milang.MiLang(err_scr)
        self.assertFalse(mi.code)
        print('Error location', mi.error_line, mi.error_col)

    def test_success(self):
        mi = milang.MiLang(ok_scr)
        self.assertTrue(mi.code)

    def test_do(self):
        mi = milang.MiLang(exe_scr)
        self.assertTrue(mi.code)
        mi.do()
        self.assertEqual(mi.env.value, 256)

    def test_exploit(self):
        for scr in exploits:
            mi = milang.MiLang(scr[0])
            self.assertFalse(mi.code, scr[1])

if __name__ == "__main__":
    unittest.main(verbosity=2)
