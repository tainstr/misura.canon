#!/usr/bin/python
# -*- coding: utf-8 -*-
import unittest
from misura import parameters as params
from misura.canon import option


c1=params.testdir+'storage/Conf.csv'
c2=params.testdir+'storage/Conf2.csv'
tmp=params.testdir+'storage/tmpfile'
db=params.testdir+'storage/tmpdb'

c3=params.mdir+'conf/MeasureFlex.csv'
c4=params.mdir+'conf/Standard.csv'
		
		
class Conf(unittest.TestCase):
	"""Tests the option.Conf object"""
	def test_setattr(self):
		s=option.CsvStore(c1)
		c=option.Conf(s.desc)
		self.assertEqual(c['temp'],25)
		self.assertFalse(c.has_key('csunit'))
		c.setattr('temp','csunit','kelvin')
		e=c.gete('temp')
		self.assertEqual(e['csunit'],'kelvin')
		self.assertEqual(e['current'],25)
		
	

if __name__ == "__main__":
	unittest.main() 