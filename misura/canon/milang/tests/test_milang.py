#!/usr/bin/python
# -*- coding: utf-8 -*-
"""Test per Mis
ura Language."""
import unittest
from misura.canon import milang

data_scr="""
t0=mi.Raises('cohe',80)
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

class MiLang(unittest.TestCase):
	mi=milang.MiLang(data_scr)
	def test_todo(self):
		assert False

if __name__ == "__main__":
	unittest.main(verbosity=2) 