#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
API for common data operations on local or remote HDF files.
"""

from scipy.interpolate import UnivariateSpline, interp1d
import tables
import numpy as np
import functools
import numexpr as ne
from .. import csutil
from ..csutil import lockme
from .. import reference
ne.set_num_threads(8)

class DataOperator(object):
	_zerotime=-1
	tlimit=False
	limit={}
	limit_enabled=set([])
	@property
	def zerotime(self):
		"""Time at which the acquisition stats. 
	No data will be evaluate if older than zerotime."""
		if self.test is False:
			return 0
		if self._zerotime<0:
			print 'ask zerotime'
			if not self.has_node('/conf'):
				print 'NO CONF NODE'
				return 0
			self._zerotime=self.get_node_attr('/conf','zerotime')
		return self._zerotime
	
	def get_zerotime(self):
		print 'get_zerotime'
		return self.zerotime
	
	def set_time_limit(self,start_time=0,end_time=-1): 
		if start_time==0 and end_time==-1:
			self.tlimit=False
			self.limit={}
			return
		print 'setting time limit',start_time,end_time
		if start_time<=self.zerotime:
			start_time=self.zerotime
		if self.tlimit and abs(self.tlimit[0]-start_time)+abs(self.tlimit[1]-end_time)>10e-6:
			# Erase all recorded limits!
			self.limit={}
		self.tlimit=(start_time,end_time)
		print 'time limit set to',self.tlimit
		return True
	
	def get_time_limit(self):
		return self.tlimit
	
	@lockme
	def set_limit(self,path,enable=False):
		"""Enable time limits for data operations on `path`."""
		# unbound get_time
		if path in self.limit_enabled:
			self.limit_enabled.remove(path) 
		if not enable:
			return enable
		if not self.tlimit or self.tlimit==(0,-1):
			print 'no time limits defined'
			self.tlimit=False
			return False
		if self.limit.has_key(path):
			self.limit_enabled.add(path)
			print 'returning cached limit',path,self.limit[path]
			return self.limit[path]


		start_time,end_time=self.tlimit	
		start=self._get_time(path,start_time)	
		if end_time>start_time:
			end=self._get_time(path,end_time)
		else:
			end=None
		print 'enabled limit',path,start,end
		self.limit[path]=slice(start,end)
		self.limit_enabled.add(path)
		return enable
	
	def get_limit(self,path):
		"""Get current time and index limits for curve path"""
		if not self.tlimit:
			return False
		if path not in self.limit_enabled:
			return False
		r = self.limit.get(path,False)
		if r is False:
			try:
				self._lock.release()
			except: 
				pass
			self.set_limit(path,True)
			return self.limit[path]
		return r
	
	def _xy(self,path=False,arr=False):
		"""limit: limiting slice"""
		x=False
		node=False
		if path is not False:
			node=self.test.get_node(path)
			arr=node
			x=arr.cols.t
			y=arr.cols.v
		else:
			x=arr[:,0]
			y=arr[:,1]
		if len(arr)==0:
			print 'Empty dataset',path
			if node: 
				node.close()
			return False,False
		if node: 
			node.close()
		if x is False:
			return False,False
		limit=self.limit.get(path,False)
		if limit:
			print 'limiting',len(x),limit
			return x[limit],y[limit]
		return x,y	
	
	@lockme
	def col(self, path, idx_or_slice=None,raw=False):
		"""Reads an array in the requested slice. If endIdx is not specified, reads just one point."""
		n=self.test.get_node(path)
		node=n
		lim=self.get_limit(path)
		if lim:
			n=n[lim]
		else:
			n=n[:]
		# Convert to regular array (we could convert to dict for fields?)
		if not raw:
			n=n.view(np.float64).reshape(n.shape + (-1,))
		slc=False
		if idx_or_slice is not None:
			slc=csutil.toslice(idx_or_slice)
			n=n[slc]
		node.close()
		return n
		
	def _col_at(self, path, idx, raw=False):
		"""Retrive single index `idx` from node `path`"""
		node=self.test.get_node(path)
		n=node[idx]
		if not raw:
			n=n.view(np.float64).reshape(n.shape + (-1,))
		node.close()
		return n

	@lockme
	def col_at(self, *a,**k):
		"""Retrive single index `idx` from node `path`, locked"""
		return self._col_at(*a,**k)
		
	def clean_start(self,g, start):
		# FIXME: Cut start limit. Send bug to pytables
		while len(g)>0:
			if g[0]<start:
				g.pop(0)
				continue
			break
		return g
	
	def find_nearest_cond(self,tab,s,f=2.,limit=slice(None,None,None)):
		"""Search for the nearest value to `s` in table `tab`, 
		by iteratively reducing the tolerance by a factor of `f`."""
		# Try exact term
		g=tab.get_where_list('v==s',stop=limit.stop)
		g=self.clean_start(g,limit.start)
		if len(g)>0:
			return g[0]
		
		ur=max(tab.cols.v)-s # initial upper range
		lr=s-min(tab.cols.v) # initial lower range
		last=None
		
		while True:
			# Tighten upper/lower ranges
			ur/=f
			lr/=f
			g=tab.get_where_list('((s-lr)<v) & (v<(s+ur))',stop=limit.stop)
			# Found!
			if g is None or len(g)==0:
				if last is None:
					return None
				return last[0]
			# FIXME: Cut start limit
			while limit.start:
				if len(g)==0:
					if last is None: 
						return None
					return last[0]
				if g[0]<limit.start:
					g.pop(0)
					continue
				break
			# Save for next iter
			last=g
			if ur+lr<0.0000000001:
				return None
		
			
	@lockme
	def search(self,path,op,cond='x==y',pos=-1):
		"""Search dataset path with operator `op` for condition `cond`"""
		print 'searching in ',path,cond
		tab=self.test.get_node(path)
		x, y=tab.cols.t, tab.cols.v
		limit=self.limit.get(path,slice(None, None, None))
		y,m=op(y)
		last=-1
		# Handle special cases
		if cond=='x>y': # raises
			if y[0]>m:
				last=0
			elif max(y)<m:
				tab.close()
				return False
			cond='y>m'
		elif cond=='x<y': # drops
			if y[0]<m:
				last=0
			elif min(y)>m:
				tab.close()
				return False
			cond='y<m'
		elif cond=='x~y':
			last=self.find_nearest_cond(tab,m,limit=limit)
			if last is None:
				tab.close()
				return False
		else:
			cond='y==m'
		if last<0:
			last=tab.get_where_list(cond,stop=limit.stop)
			# WARNING: start selector is not working. 
			#TODO: Send bug to pytables!
			while limit.start:
				if len(last)==0:
					last=None
					break
				if last[0]<limit.start:
					last.pop(0)
					continue
				break
				
			print 'get_where_list', path, cond, m, last, limit
			if last is None or len(last)==0:
				print 'FAILED SEARCH', path, cond, m, limit
				tab.close()
				return False
			last=last[0]
		print 'done searching',path,last, len(y), x[last], y[last], m,  cond
		tab.close()
		return last,x[last],y[last]
	
	def max(self,path):
		op=lambda y: (y, max(y))
		return self.search(path,op)
	
	def min(self,path):
		op=lambda y: (y, min(y))
		return self.search(path,op)
	
	def nearest(self,path,val):
# 		def op(y):
# 			y=abs(y-val)
# 			return y,min(y)
		op=lambda y: (y,val)
		return self.search(path,op,cond='x~y')

	def equals(self,path,val, tol=10**-12):
		op=lambda y: (y,val)
		r=self.search(path,op)
		if not r: 
			return False
		i, xi, yi=r
		if abs(yi-val)>tol:
			return False
		return i, xi, yi
		
	def drops(self,path,val):
#		cond=lambda a,b: a<b
		cond='x<y'
		op=lambda y: (y,val)
		print 'drops',path,val
		return self.search(path,op,cond,pos=0)
	
	def rises(self,path,val):
#		cond=lambda a,b: a>b
		cond='x>y'
		op=lambda y: (y,val)
		print 'rises', path, val
		return self.search(path,op,cond,pos=0)
	
	
	def _get_time(self,path,t, get=False,seed=None):
		"""Optimized search of the nearest index to time `t` using the getter function `get` and starting from `seed` index."""
		n=self.test.get_node(path)
#		lim=self.get_limit(path)
#		if lim:
#			n=n[lim]
		if get is False:
			get=lambda i: n[i][0]
		else:
			get=functools.partial(get,n)
		idx=csutil.find_nearest_val(n, t, get=get, seed=seed)
		n.close()
		return idx
	
	@lockme
	def get_time(self,*a,**k):
		return self._get_time(*a,**k)
	
	@lockme
	def get_time_profile(self,path,t):
		return self._get_time(path,t,get=reference.Profile.unbound['decode_time'])
	
	@lockme
	def get_time_image(self,path,t):
		return self._get_time(path,t,get=reference.Image.unbound['decode_time'])
		
	
	@lockme
	def spline_col(self,path=False,startIdx=0,endIdx=-1, time_sequence=[0], arr=False, k=3):
		"""Returns a 1D interpolated version of the array, as per """
		x,y=self._xy(path,arr)
		if x is False:
			return False
		print 'building spline',path
		f=UnivariateSpline(x,y,k=k)
		print 'interp'
		r=f(time_sequence)
		return r
	
	@lockme
	def interpolated_col(self,path=False,startIdx=0,endIdx=None, time_sequence=[0], arr=False,kind='cubic'):
		x,y=self._xy(path,arr)
		if x is False:
			return False
		if startIdx==endIdx==-1:
			# Detect from time_sequence
			startIdx=self._get_time(path,time_sequence[0]-1)
			endIdx=self._get_time(path,time_sequence[-1]+1,seed=startIdx)
		s=slice(startIdx,endIdx)
		print 'Interpolating ',path,s
		f=interp1d(x[s], y[s], kind=kind, bounds_error=False, fill_value=0)
		print 'interp'
		r=f(time_sequence)
# 		print 'interpolate',x,time_sequence,r
		return r		
	
	
	
	
	
