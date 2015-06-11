# -*- coding: utf-8 -*-
"""Indexing hdf5 files"""
# NOTICE: THIS FILE IS ALSO PART OF THE CLIENT. IT SHOULD NOT CONTAINS REFERENCES TO THE SERVER OR TWISTED PKG.
ext='.h5'

import os
from .. import csutil 

from interface import SharedFile


class FileManager(object):
	def __init__(self, store=False,prefix=''):
		if store is False:
			self.log=csutil.FakeLogger()
		else:
			self.log=store.log
		self.store=store
		self.tests={}	# uid: SharedFile mapping
		self.uids={}	# uid: filepath mapping
		self.paths={}	# filepath: uid mapping
		
	def open(self,prefix):
		# open by path
		if prefix.startswith('/'):
			f=self.path(prefix)
			if f is False:
				f=self.open_file(prefix)
				if f is False:
					print 'file not found',prefix
					return False
			uid=self.paths[prefix]
		# open by uid
		else:
			f=self.uid(prefix)
			if f is False:
				f=self.open_uid(prefix)
				if f is False:
					print 'Failed opening by uid', prefix
					return False
			uid=prefix
		f=self.tests[uid]
		return f
	
	def open_uid(self,uid, readLevel=3):
		"""Open a file corresponding to `uid` and associates it to the current session"""
		# Search the filename corresponding to the requested UID
		fn=self.uids.get(uid,False)
		if (fn is False) and self.store: 
			fn=self.store.searchUID(uid)
			if fn: self.uids[uid]=fn
		if fn is False:
			self.log.info('No test found with the requested UID',uid)
			return False
		# Recall an already opened interface, it it exists
		s=self.tests.get(uid,False)
		# Close and reopen it
		if s: s.close()
		s=SharedFile(fn,uid=uid,log=self.log)
		self.tests[uid]=s
		self.uids[uid]=fn
		self.paths[fn]=uid
		return True
	
	def open_file(self,fpath,uid=''):
		"""Opens a SharedFile for fpath and assign a uid"""
		if not os.path.exists(fpath):
			self.log.error('Requested file does not exist',fpath)
			return False
		s=SharedFile(fpath,uid=uid,log=self.log)
		# Read updated uid
		uid=s.get_uid()
		self.tests[uid]=s
		self.uids[uid]=fpath
		self.paths[fpath]=uid
		return True	
	
	def uid(self,uid):
		"""Retrieve a file by uid"""
		return self.tests.get(uid,False)
	
	def path(self,p):
		"""Retrieve a file by path"""
		u=self.paths.get(p,False)
		if u is False: 
			return u
		return self.tests.get(u,False)
	
	def purge(self,hdf):
		"""Remove hdf from registry"""
		uid=hdf.get_uid()
		path=hdf.get_path()
		if hdf.isopen():
			hdf.close()
		if self.uid.has_key(uid):
			del self.uid[uid]
		if self.path.has_key(path):
			del self.path[path]
		return uid,path
	
	def close_uid(self,uid):
		f=self.uid(uid)
		if f:
			f.close()
		return True
	
	def close(self):
		for ti in self.tests.itervalues():
			if ti:
				ti.close()
		self.tests={}
		self.uids={}
		