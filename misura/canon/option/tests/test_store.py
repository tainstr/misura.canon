#!/usr/bin/python
# -*- coding: utf-8 -*-
import unittest
import os
from misura.canon import option
from misura import parameters as params
import sqlite3


c1=params.testdir+'storage/Conf.csv'
c2=params.testdir+'storage/Conf2.csv'
tmp=params.testdir+'storage/tmpfile'
db=params.testdir+'storage/tmpdb'

c3=params.mdir+'conf/MeasureFlex.csv'
c4=params.mdir+'conf/Standard.csv'


class CsvStore(unittest.TestCase):
	
	def test_merge_file(self):
		st=option.CsvStore(kid='ciao')
		st.merge_file(c1)
		self.assertEqual(st.desc['name']['kid'],'ciaoname')
	
#	@unittest.skip('')
	def test_write_file(self):
		st=option.CsvStore(kid='ciao')
		st.merge_file(c1)
		st.write_file(tmp)
		st1=option.CsvStore(kid='ciao')
		st1.merge_file(tmp)
		self.assertEqual(st.desc,st1.desc)

#	@unittest.skip('')
	def test_imports(self):
		st=option.CsvStore(kid='ciao')
		st.read_file(c3)
		
#	@unittest.skip('')
	def test_multiline(self):
		"""Controlla che lettura e salvataggio multiline funzionino correttamente"""
		st=option.CsvStore(kid='ciao')
		st.read_file(c4)	# Standard contiene molti multiline per via degli Script
		opt0=st.desc['maxT']
		st.write_file('out.csv')
		st.read_file('out.csv')
		opt1=st.desc['maxT']
		self.assertEqual(opt0,opt1)
		print opt0
		print opt1

	def test_priority(self):
		st=option.CsvStore(c1)
		self.assertTrue(st.desc.has_key('name'))
		e=st.desc['PQF']
		self.assertEqual(e['priority'], 12, msg="Wrong priority. real=%i, teor=%i" % (e['priority'], 12))
		st.validate()
		e=st.desc['PQF']
		self.assertEqual(e['priority'], 12, msg="Wrong priority after validation. real=%i, teor=%i" % (e['priority'], 12))
		

class SqlStore(unittest.TestCase):
	@classmethod
	def setUpClass(cls):
		if os.path.exists(db): os.remove(db)
		cls.conn=sqlite3.connect(db)	
	
#	@unittest.skip('')
	def test_tables(self):
		st0=option.CsvStore(kid='ciao')
		st0.merge_file(c1)
		st=option.SqlStore(kid='ciao')
		st.desc=st0.desc
		k0=set(st.desc.iterkeys())
		cursor=self.conn.cursor()
		st.write_table(cursor,'conf1')
		self.conn.commit()
		cursor.execute('select handle from conf1')
		r=cursor.fetchall()
		k1=set([eval(k[0]) for k in r])
		self.assertEqual(k0,k1)
		
		st2=option.SqlStore(kid='ciao')
		st2.read_table(cursor,'conf1')
		self.assertEqual(st.desc,st2.desc)
		
class ListStore(unittest.TestCase):
	
	def setUp(self):
		self.teor={'opt': option.Option(**{'priority': 1, 
				'handle': 'opt', 'name': 'Opt', 'parent': False, 'attr': [], 
				'writeLevel': 1, 'current': 0, 'factory_default': 0, 'flags': {}, 
				'readLevel': 0, 'type': 'Integer', 'unit': 'None', 'kid': '::opt'})}
		self.lst=[{'handle':'opt','type':'Integer'}]
	
	def test_read_list(self):
		s=option.ListStore(self.lst)
		self.assertTrue(s.desc.has_key('opt'))
		self.assertEqual(s.desc,self.teor)
		
	def test_read(self):
		s=option.ListStore.read(self.lst)
		self.assertTrue(s.has_key('opt'))
		self.assertEqual(s,self.teor)
		# Current updating  feature
		self.lst.append({'opt':1})
		self.teor['opt']['current']=1
		s=option.ListStore.read(self.lst)
		self.assertEqual(s, self.teor)
		

if __name__ == "__main__":
	unittest.main() 
