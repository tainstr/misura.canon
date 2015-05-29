#!/usr/bin/python
# -*- coding: utf-8 -*-
import unittest
from misura import parameters as params
from misura.canon import indexer
print 'Importing test_indexer'

paths=[params.testdir+'storage']

dbPath=params.testdir+'storage/db'

dbPath='/media/essmeridia/deploy/misura4/opt/shared_data/test.sqlite'
paths=['/media/essmeridia/deploy/misura4/opt/shared_data']
# dbPath='/home/daniele/Scrivania/tests/test.sqlite'
# paths=['/home/daniele/Scrivania/tests']
def setUpModule():
	print 'Starting',__name__
	
class Indexer(unittest.TestCase):
	
	@classmethod
	def setUpClass(cls):
		cls.store=indexer.Indexer(dbPath,paths)
		
	def test_0_rebuild(self):
		self.store.rebuild()
		
	def test_1_header(self):
		h=self.store.header()
		
	def test_2_listMaterials(self):
		self.store.listMaterials()
		
	def test_3_query(self):
		r=self.store.query()
		instr=r[0][5]
		n=0
		for e in r: 
			if e[5]==instr: n+=1
		r=self.store.query({'instrument':instr})
		self.assertEqual(len(r),n)
		r=self.store.query({'instrument':'pippo'})
		self.assertEqual(len(r),0)
		
	def test_4_searchUID(self):
		r=self.store.query()
		path=r[0][0]
		uid=r[0][2]
		r=self.store.searchUID(uid)
		self.assertEqual(r,path)
		
if __name__ == "__main__":
	unittest.main()  
	
