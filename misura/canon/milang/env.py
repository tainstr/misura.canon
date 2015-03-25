#!/usr/bin/python
# -*- coding: utf-8 -*-
"""Misura Language or Mini Language. 
Secure minimal Python language subset for conditional evaluation of numerical datasets."""

import types
import exceptions
import numpy as np


class ExitException(exceptions.Exception):
	pass

class MiLangEnvironment(object):
	"""Execution environment"""
	whitelist=[]
	sub={}
	handle=''
	"""Current execution option handle."""
	
	def __init__(self):
		for name in dir(self):
			if name.startswith('_'): continue
			if name.endswith('_'): continue
			if name=='hdf': continue
			func=getattr(self,name)
			if type(func)!=types.MethodType:
				continue
			self.whitelist.append(name)
	
	def _addEnvironment(self,name,env):
		setattr(self,name,env)
		self.whitelist+=env.whitelist
		self.sub[name]=env
	
class BaseEnvironment(MiLangEnvironment):
	"""Execution environment where basic point characterization, metadata and condition evaluation functions are defined."""
	whitelist=['len','max','min']
	"""Allowed function calls"""
	temp=None
	"""Identified temperature"""
	time=None
	"""Identified time"""
	value=None
	"""Additional output value"""
	comment=""
	"""Log comment"""
		
	def Exit(self,msg=False):
		if msg is not False:
			self.Log(msg)
		raise ExitException(msg)
		
	def T(self,T):
		self.temp=float(T)
	def t(self,t):
		self.time=float(t)	
	def Value(self,v):
		self.value=float(v)
	def Log(self,s):
		self.comment=str(s)
		print self.comment
		
	def Where(self,cond):
		"""Returns the first truth value of cond"""
		w=np.where(cond)[0]
		if len(w)==0: return -1
		return w[0]
		
	def dummy(self,a,b):
		"""Test function"""
		return a+b
	
	def _reset(self):
		self.temp, self.time, self.value = [None]*3
		self.comment=""
