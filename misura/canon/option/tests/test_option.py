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

print 'Importing test_option'

def setUpModule():
	print 'Starting test_option'
	

class Option(unittest.TestCase):
	"""Tests the basic option.Option object"""
	def test_option(self):
		o=option.Option(current=0,handle='test',type='Integer')
		self.assertEqual(o.get(),0)
		self.assertEqual(o.get(),o['current'])
		self.assertEqual(o.get(),o.get('current'))
		o['csunit']='minute'
		self.assertEqual(o.get(),0)
		self.assertEqual(o['csunit'],'minute')
		
	
		

		

if __name__ == "__main__":
	unittest.main() 
