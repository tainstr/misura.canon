# -*- coding: utf-8 -*-
"""Option persistence on HDF files."""
import tables
from misura.parameters import cfilter
from reference import Reference
import numpy as np
from scipy.interpolate import interp1d, LSQUnivariateSpline

class Array(Reference):
	fields=[('t','float64'),('v','float64')]
	def create(self, fields=False):
		"""Create an EArray (enlargeable array) as data storage"""
		f=Reference.create(self)
		if not f:
			return False
		if fields:
			self.fields=fields
		self.outfile.create_table(where=f,
									name=self.handle,
									description=np.dtype(self.fields),
									title=self.name,
									filters=cfilter,
									createparents=True,
									reference_class=self.__class__.__name__)
# 		print 'created',f,self.folder,self.handle
		self.path=self.folder+self.handle
# 		print 'done',self.path
		self.outfile.flush()
		# Create the summary mirror
		if (not self.path.startswith('/summary')) and len(self.fields)==2:
# 			print 'Creating summary',self.path
			self.summary=Array(self.outfile, '/summary'+self.folder, opt=self.opt)
		return True
	
	def open(self,folder):
		"""Open an existing Array in `folder` with its summary Array"""
		Reference.open(self,folder)
		# Open the summary mirror
		if not self.path.startswith('/summary') and len(self.fields)==2 :
			self.summary=Array(self.outfile,'/summary'+self.path)
	
	@classmethod
	def encode(cls,t,dat):
		if len(cls.fields)==2:
			return np.array([(t,dat)],dtype=cls.fields)
		else:
			return np.array([tuple([t]+dat)],dtype=cls.fields)
	@classmethod
	def decode(cls,dat):
		if len(dat)==1:
			dat=dat[0]
		n=len(dat)	
		if n!=len(cls.fields):
			return None
		if n==2:
			return list(dat)
		return Reference.decode(tuple(dat))
	
	#TODO: move into OutputFile to avoid IPC (performance quite ok anyway)
	def interpolate(self,step=1,kind=1):
		"""Array interpolation for summary synchronization."""
		vt=Reference.interpolate(self,step)
		if vt is False:
			return False
		# Value sequence
		oldi=self.get_time(vt[0]-step) # starting from the oldest time minus step
		# Check if we have enough points to interpolate
		if len(self)-oldi<5:
			return False
		# If possible, go back one more point, for interpolation safety
		if oldi>1:	oldi-=1
		# Decode values and separate time and value vectors
		dat=self[oldi:]
# 		print 'Getting data',self.path,dat,vt
		dat=np.array(dat)
		dat=dat.transpose()
		# Interpolate time and value - interp1d version
		#f=interp1d(dat[0],dat[1],kind=kind)
		# Build a linear spline using vt points as knots
		#f=LSQUnivariateSpline(dat[0],dat[1],vt, k=kind)
		# Do a linear fitting
		(slope,const),res,rank,sing,rcond=np.polyfit(dat[0],dat[1], kind, full=True)
		# Build a vectorized evaluator
		f=np.vectorize(lambda x: slope*x+const)
		while vt[0]<dat[0][0] and len(vt)>1:
			vt=vt[1:]
		while vt[-1]>dat[0][-1] and len(vt)>1:
			vt=vt[:-1]
		if len(vt)<=1:
			return False
		try:
			# Interpret time series
			out=f(vt)
		except:
			print 'Array.interpolate',self.path,vt,dat
			raise
		# Encode in (t,v) append-able list
		out=np.array([vt,out]).transpose()
# 		print 'Appending',self.summary.path,len(out),out
		self.summary.commit(out)
		
	
class Boolean(Array):
	"""A True/False value"""
	fields=[('t','float64'),('v','uint8')]
	
class Rect(Array):
	"""An Array with 5 columns, one for the time, 
	4 for the coordinates of a rectangle"""
	fields=[('t','float64'),('x','uint16'),('y','uint16'),('w','uint16'),('h','uint16')]
	
class Point(Array):
	"""An Array with 3 columns, one for the time, 
	2 for x,y integers"""
	fields=[('t','float64'),('x','uint16'),('y','uint16')]
	
class Meta(Array):
	"""An Array reference with 4 columns, one for the time, 
	3 for value,time,temp keys of a Meta option type"""
	fields=[('t','float64'),('value','float64'),('time','float64'),('temp','float64')]
	@classmethod
	def encode(cls,t,dat):
		"""Flatten the Meta dictionary into a float list of t,value,time,temp"""
		if len(dat)>3:
			print 'wrong meta',dat,len(dat)
			return None
		r=(t,dat['value'],dat['time'],dat['temp'])
		return np.array([r],dtype=cls.fields)
	@classmethod
	def decode(cls,dat):
		"""Rebuild the Meta dictionary"""
		if len(dat)==1: 
			dat=dat[0]
		if len(dat)!=len(cls.fields):
			return None
		return [dat[0],{'value':dat[1],'time':dat[2],'temp':dat[3]}]

