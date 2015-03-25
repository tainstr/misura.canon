#!/usr/bin/python
# -*- coding: utf-8 -*-
import unittest
from misura import parameters as params
from misura.canon import indexer
print 'Importing test_indexer'

paths=[params.testdir+'storage']
dbPath=params.testdir+'storage/db'

def setUpModule():
	print 'Starting',__name__

		
class FileManager(unittest.TestCase):
	uid='d03bc400d91fd0a4f2399571f8052808'
	@classmethod
	def setUpClass(c):
		store=indexer.Indexer(dbPath,paths)
		print 'FileManager',dbPath,paths,store.rebuild()
		c.m=indexer.FileManager(store)
	
	def test_0_open_uid(self):
		r=self.m.open_uid(self.uid)
		self.assertTrue(r)
		print r
		
	def test_1_uid(self):
		s=self.m.uid(self.uid)
		self.assertTrue(s)
		
	@classmethod	
	def tearDownClass(c):
		s=c.m.uid(c.uid)
		if s: s.close()
		c.m.close()	

if __name__ == "__main__":
	unittest.main()  
