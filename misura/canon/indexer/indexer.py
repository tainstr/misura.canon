# -*- coding: utf-8 -*-
"""Indexing hdf5 files"""
# NOTICE: THIS FILE IS ALSO PART OF THE CLIENT. IT SHOULD NOT CONTAINS REFERENCES TO THE SERVER OR TWISTED PKG.
ext='.h5'

import hashlib
import os
import cPickle as pickle
from traceback import print_exc
import sqlite3
import functools

import tables
from tables.nodes import filenode

from .. import csutil, option

from filemanager import FileManager
import digisign

testColumn=('file', 'serial', 'uid', 'id', 'date', 'instrument', 'flavour', 'name', 'elapsed', 'nSamples' , 'comment','verify')
testColumnDefault=['file','serial','uid', 'id', 'date', 'instrument', 'flavour', 'name', 1, 1 , 'comment',0]
testColDef=('text','text','text','text','text','text','text','text', 'real','integer','text','bool')
testTableDef='''(file text, serial text, uid text, id text, date text, instrument text, flavour text,
		name text, elapsed real, nSamples integer, comment text,verify bool)'''
sampleTableDef='''(file text, ii integer, idx integer, material text, name text, comment text, 
		dim integer, height integer, volume integer, 
		sintering real, softening real, sphere real, halfSphere real, melting real )'''
testColConverter={}
colConverter={'text':unicode,'real':float,'bool':bool,'integer':int}
for i,n in enumerate(testColumn):
	testColConverter[n]=colConverter[testColDef[i]]


def dbcom(func):
	"""Decorator to open db before operations and close at the end."""
	@functools.wraps(func)
	def safedb_wrapper(self, *args, **kwargs):
		try:
			self.open_db()
			return func(self, *args, **kwargs)
		finally:
			self.close_db()
	return safedb_wrapper

	

class Indexer(object):
	public=['rebuild','searchUID','update','header','listMaterials',
		'query','remove','get_len','list','get_dbpath']
	cur=False
	conn=False
	addr='LOCAL'
	def __init__(self,dbPath=False,paths=[],log=False):
		self.dbPath=dbPath
		self.paths=paths
		if log is False:
			log=csutil.FakeLogger()
		self.log=log
		self.test=FileManager(self)
		self.dbPath=dbPath
		if dbPath and not os.path.exists(dbPath):
			self.rebuild()
			
	@dbcom
	def execute(self,query,*a,**kw):
		self.cur.execute(query,*a,**kw)
		self.conn.commit()
		
	@dbcom
	def execute_fetchall(self,query,*a,**kw):
		g=self.cur.execute(query,*a,**kw)
		return g.fetchall()
		
	@dbcom
	def execute_fetchone(self,query,*a,**kw):
		g=self.cur.execute(query,*a,**kw)
		return g.fetchone()
			
	def open_db(self,db=False):
		if not db: 
			db=self.dbPath
		self.dbPath=db
		if not self.dbPath: 
			return False
		self.conn=sqlite3.connect(self.dbPath, detect_types=sqlite3.PARSE_DECLTYPES)
		self.cur=self.conn.cursor()
		self.cur.execute("CREATE TABLE if not exists test "+testTableDef)
		self.cur.execute("CREATE TABLE if not exists sample "+sampleTableDef)
		# Sync tables
		self.cur.execute("create table if not exists sync_exclude "+testTableDef)
		self.cur.execute("create table if not exists sync_queue "+testTableDef)
		self.conn.commit()
		return True
		
	def close_db(self):
		if self.cur:
			self.cur.close()
			self.conn.close()
		self.cur=False
		self.conn=False
		return True
		
	def close(self):
		return self.close_db()
		
	def searchUID(self,uid,full=False):
		"""Search `uid` in tests table and return its path or full record if `full`"""
		uid=str(uid)
		conn=sqlite3.connect(self.dbPath)
		cur=conn.cursor()
		try:
			cur.execute('SELECT file FROM test WHERE uid=?', [uid])	
		except sqlite3.OperationalError:
			self.close_db()
			self.rebuild()
			return False
		r=cur.fetchall()
		r=list(set(r))
		if len(r)==0:
			return False
		if len(r)>1:
			self.log.warning('Found duplicate UIDs',r)
		# Return full line
		if full:
			return r[0]
		return r[0][0]
	
	def search_path(self,path):
		path=str(path)
		conn=sqlite3.connect(self.dbPath, detect_types=sqlite3.PARSE_DECLTYPES)
		cur=conn.cursor()	
		cur.execute('SELECT file FROM test WHERE path=?', [path])	
		r=cur.fetchall()
		r=list(set(r))
		if len(r)==0:
			return False
		if len(r)>1:
			self.log.warning('Found duplicate paths',r)
		return r[0][0]

	def rebuild(self):
		"""Completely recreate the SQLite Database indexing all test files."""
		if not self.dbPath: return False
		if os.path.exists(self.dbPath): 
			os.remove(self.dbPath)
		self.open_db(self.dbPath)
		
		conn=self.conn
		cur=self.cur
		self.cur=cur
		cur.execute("DROP TABLE IF EXISTS test")
		cur.execute("CREATE TABLE test "+testTableDef)
		cur.execute("DROP TABLE IF EXISTS sample")
		cur.execute("CREATE TABLE sample "+sampleTableDef)
		cur.execute("DROP TABLE IF EXISTS sync_exclude")
		cur.execute("DROP TABLE IF EXISTS sync_queue")
		conn.commit()
		self.close_db()
		tn=0
		for path in self.paths:
			for root, dirs, files in os.walk(path):
				for fn in files:
					if not fn.endswith(ext): continue
					fp=os.path.join(root,fn)
					print 'Appending',fp
					tn+=self.appendFile(fp,fn)	
		
		return 'Done. Found %i tests.' % tn
	
	
	def appendFile(self,fp,fn=False):
		#TODO: remove unused fn argument!!!
		if not os.path.exists(fp):
			print 'File not found',fp
			return False
		if not fn:
			fn=os.path.basename(fp)
		r=0
		t=False
		try:
			t=tables.openFile(fp,mode='r+')	
			if not getattr(t.root, 'conf', False):
				self.log.debug('Tree configuration not found',fp)
				t.close()
				return False
			r=self._appendFile(t,fp,fn)
		except:
			print_exc()
		if t: 
			t.close()
		return r
		
	@dbcom
	def _appendFile(self, t, fp,fn):
		"""Inserts a new file in the database"""
		#FIXME: inter-thread #412
		cur=self.cur
		conf=getattr(t.root,'conf',False)
		# Load configuration
		node=filenode.openNode(t.root.conf, 'r')
		node.seek(0)
		tree=node.read()
		node.close()
		tree=pickle.loads(tree)	
		
		###
		# Test row
		###
		test={}	
		test['file']=fp
		instrument=conf.attrs.instrument
		test['instrument']=instrument
		if not tree.has_key(instrument):
			print tree.keys()
			self.log.debug('Instrument tree missing')
			return False
		for p in 'name,comment,nSamples,date,elapsed,id'.split(','):
			test[p]=tree[instrument]['measure']['self'][p]['current']
		if test['date']<=1:
			test['date']=os.stat(fp).st_ctime
		test['serial']=conf.attrs.serial
		if not getattr(conf.attrs,'uid',False): 
			self.log.debug('UID attribute not found')
			sname=tree[instrument]['measure']['id']
			test['uid']=hashlib.md5('%s_%s_%i' % (test['serial'], test['date'], sname)).hexdigest()
		else:
			test['uid']=conf.attrs.uid
		test['flavour']='Standard'
		v=[]
		for k in 'file,serial,uid,id,date,instrument,flavour,name,elapsed,nSamples,comment'.split(','):
			v.append(testColConverter[k](test[k]))
# 		ok=digisign.verify(t)
		#Performance problem: should be only verified on request.
		ok=False
		print 'File verify:',ok
		v.append(ok)
		cmd='?,'*len(v)
		cmd='INSERT INTO test VALUES ('+cmd[:-1]+')'
		print 'Executing',cmd,v
		self.cur.execute(cmd,v)
		r=cur.fetchall()
		###
		# Sample Rows
		###
		for i in range(8):
			s='sample%i' % i
			if s not in tree[instrument].keys(): break
			smp=tree[instrument][s]
			v=[fp]
			for k in 'ii,index,material,name,comment,dim,height,volume,sintering,softening,sphere,halfSphere,melting'.split(','):
				val=0
				if smp.has_key(k):
					val=smp[k]['current']
				v.append(val)
			cmd='?,'*len(v)
			cmd='INSERT INTO sample VALUES ('+cmd[:-1]+')'
			cur.execute(cmd,v)
			r=cur.fetchall()
			print 'Result:',r
		self.conn.commit()
		
		###
		# Options
		###
		s=option.SqlStore()
		s.cursor=cur
		s.write_tree(tree,preset=test['uid'])
		
		self.conn.commit()
		return True

	def update(self):
		"""Updates the database by inserting new files and removing deleted files"""
		#TODO
		pass
	
	@dbcom
	def remove(self,uid):
		fn=self.searchUID(uid)
		if not fn:
			self.log.error('Impossible delete:',uid,'not found.')
			return False
		return self.remove_file(fn)
	
	@dbcom
	def remove_file(self,fn):
		#FIXME: inter-thread #412
		e=self.cur.execute('delete from test where file=?',(fn,))
		print 'Deleted from test',e.rowcount
		e=self.cur.execute('delete from sample where file=?',(fn,))
		print 'Deleted from sample',e.rowcount
		self.conn.commit()
		if os.path.exists(fn):
			os.remove(fn)
		return True
				
	@dbcom
	def header(self):
		self.cur.execute('PRAGMA table_info(test)')
		r=self.cur.fetchall()
		print 'Indexer.header',r
		return [str(col[1]) for col in r]

	@dbcom
	def listMaterials(self):
		"""Lists all materials present in the database"""
		self.cur.execute('SELECT material FROM sample')
		r=self.cur.fetchall()
		r=list(a[0] for a in r)
		r=list(set(r))
		print 'LIST MATERIALS',r
		return r

	@dbcom
	def query(self, conditions={}):
		#FIXME: inter-thread #412
		if len(conditions)==0:
			self.cur.execute('SELECT * from test')
		else:
			cnd=[]
			vals=[]
			for k,v in conditions.iteritems():
				cnd.append(k+' like ?')
				vals.append('%'+v+'%')
			cnd=' AND '.join(cnd)
			cmd='SELECT * from test WHERE '+cnd
			self.log.debug('Executing',cmd,vals)
			self.cur.execute(cmd,vals)
		r=self.cur.fetchall()
		return r
	
	
	@dbcom
	def get_len(self):
		self.cur.execute('SELECT Count(*) FROM test')
		r=self.cur.fetchone()[0]
		return r
		
	@dbcom
	def list(self,start=0,stop=25):
		self.cur.execute('SELECT * FROM test ORDER BY rowid LIMIT ? OFFSET ?',(stop-start,start))
		r=self.cur.fetchall()
		return r
		
	def get_dbpath(self):
		return self.dbPath
		
		
		
	
	
