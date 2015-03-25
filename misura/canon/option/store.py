# -*- coding: utf-8 -*-
"""Option persistence."""
from .. import logger
from option import Option, sorter, vkeys,tosave
from traceback import format_exc
import ast
import os
tabdef="' text,'".join(vkeys)+"' text"
tabdef="'"+tabdef

class Store(object):
	"""Used for iterative Option loading from file, db, etc"""
	def __init__(self,kid=''):
		self.kid=kid
		self.desc={}
		self.log=logger.Log
		self.priority=0
		self.priorities=[]
		
	def update(self,opt):
		"""Update base KID and priority"""
		# add priority field if not present
		#TODO: remove all priority management and substitute with collections.OrderedDict
		if opt.has_key('priority'): 
			self.priority=int(opt['priority'])
		else:
			self.priority+=1
			while self.priority in self.priorities: 
				self.priority+=1
			opt['priority']=self.priority
		self.priorities.append(self.priority)
		opt.set_base_kid(self.kid)
		return opt
	
	def validate(self):
		failed={}
		if len(self.desc)==0:
			return failed
		for key,entry in self.desc.iteritems():
			entry=self.update(entry)
			if entry: self.desc[key]=entry
			else:
				failed[key]=entry
				print 'Validation failed:', key, entry, ' -- REMOVED'
		return failed
	
	@classmethod
	def read(cls,obj):
		s=cls(obj)
		return s.desc
	

assign_sym='=>'
concat_sym='|;|'

def from_string(line):
	"""Create an Option object starting from a csv text line"""
	line=unicode(line, 'utf8', 'replace')
	if line[0]=='#' or len(line)<5 or line.startswith('import:'): 
		return False
	ents=line.split(concat_sym)
	entry={}
	for ent in ents:
		key, val=ent.split(assign_sym)
		val=ast.literal_eval(val)
		entry[key]=val
	return Option(**entry)

def to_string(opt):
	"""Encodes the option into a string"""
	entry=opt.entry
	if not tosave(entry): return False
	line=''
	for key, val in entry.iteritems():
		if key in ['kid']: continue
		line+='%s%s%r%s' % (key, assign_sym, val, concat_sym)
	line=line[0:-3]
	return line


###
# CSV FILE PERSISTENCE
###

class CsvStore(Store):
	
	def __init__(self, filename=False,kid=''):
		Store.__init__(self,kid=kid)
		self.filename=filename
		if filename:
			self.read_file(filename)
		
	def merge_file(self,filename, enable_imports=True):
		out=open(filename, 'r')	
		lin=-1
		line=''
		for raw in out:
			lin+=1
			
			if raw.startswith('#EOF'): 
				break
			if raw.endswith('\\\n'):
#				print 'continuing line'
				raw=raw.strip('\\\n')
				line+=raw+'\n'
				continue
			
			line+=raw
			
			if line.lower().startswith('import:'):
				if not enable_imports: 
					line=''
					continue
				line=line.strip('\n')
				# Removing leading whites
				line=line[7:]
				line=line.lstrip()
				if not line.endswith('.csv'): line+='.csv'
				# Relative imports
				if line.startswith('.') or not line.startswith('/'):
					path,name=os.path.split(filename)
					line=os.path.join(path,line)
				self.merge_file(line)
				line=''
				continue
			try:
				entry=from_string(line)
			except:
				self.log.error('Reading config:',filename,'\nat entry: ',lin, line, '\n', format_exc())
				line=''
				continue
			if not entry:
				line=''
				continue
			entry=self.update(entry)
			line=''
			if not entry: 
				continue
			self.desc[entry['handle']]=entry
		return self.desc
	
	def read_file(self,filename=False):
		"""Start a new import of a configuration file"""
		if not filename: filename=self.filename
		if not os.path.exists(filename):
			self.log.error('Read: non existent path', filename)
			return False
		self.filename=filename
		self.desc={}
		self.merge_file(filename)
		return self.desc				
				
	def write_file(self,filename=False):
		if not filename: filename=self.filename
		out=open(filename, 'w')
		values=self.desc.items()
		values.sort(sorter)
		prio=0
		for key, entry in values:
			prio+=1; entry['priority']=prio
			line=to_string(entry)
			if not line: continue
			out.write(line+'\n')
		out.close()	
		
###
#  SQL PERSISTENCE
###

def from_row(row):
	"""Create an Option object starting from a database row"""
	e={}
	print row
	for i,k in enumerate(vkeys):
		if row[i]=='': continue
		e[k]=ast.literal_eval(row[i])
	return Option(**e)

def to_row(entry):
	"""Encode the option into a database row"""
	if not tosave(entry): return False
	r=[]
	for k in vkeys:
		if entry.has_key(k):
			r.append(repr(entry[k]))
		else:
			r.append('')
	return r

		
class SqlStore(Store):	
	def read_table(self,cursor,tabname):
		cursor.execute("SELECT * from "+tabname)
		r=cursor.fetchall()
		self.desc={}
		for row in r:
			entry=from_row(row)
			entry=self.update(entry)
			if not entry: continue
			self.desc[entry['handle']]=entry
		return self.desc
	
	def write_table(self, cursor,tabname,desc=False):
		# Create the table
		if not desc: desc=self.desc
		cursor.execute("drop table if exists "+tabname)
		cmd="create table " + tabname + " ("+tabdef+");"
		print cmd
		cursor.execute(cmd)
		
		# Prepare the insertion command
		icmd='?,'*len(vkeys)
		icmd='INSERT INTO '+tabname+' VALUES ('+icmd[:-1]+')'
		
		# Reorder the options by priority
		values=desc.items()
		values.sort(sorter)
		prio=0
		
		# Write the options
		for key, entry in values:		
			prio+=1; entry['priority']=prio
			line=to_row(entry)
			if not line:
				print 'skipping line',key 
				continue
			print entry.keys(),tabdef
			print icmd,line
			cursor.execute(icmd,line)
		return True

		
class ListStore(Store):	
	def __init__(self,lst=[]):
		Store.__init__(self)
		self.read_list(lst)
		
	def read_list(self,lst):
		for entry in lst:
			ks=entry.keys()
			# intercept simple update requests: opt is key:val
			if len(ks)==1:
				k=ks[0]
				if not self.desc.has_key(k):
					print 'Missing update key',entry
					continue
				print 'Default modified to', k, entry[k]
				self.desc[k]['current']=entry[k]
				continue
			# Else, add the option
			entry=self.update(Option(**entry))
			if not entry: continue
			self.desc[entry['handle']]=entry
		return self.desc
		
	
