# -*- coding: utf-8 -*-
"""Option persistence on HDF files."""
import tables
from misura.parameters import cfilter
import numpy as np
from reference import Reference
#TODO: Unify commit/append!!! They are basically the same!
from variable import VariableLength, binary_cast


def decode_time(node,index):
	t=binary_cast(node[index][:4,0],'HHHH','d')[0]
	return t

class Profile(VariableLength):
	unbound=VariableLength.unbound.copy()
	unbound['decode_time']=decode_time
	
	def create(self):
		"""Create a VLArray for containing frames as variable-length sequences of t,width, pixels..."""
		f=Reference.create(self)
		if not f:
			return False
		self.outfile.create_vlarray(where=f, 
									name=self.handle,  
									atom=tables.UInt16Atom(shape=(2,)), 
									title=self.name,  
									filters=cfilter,
									createparents=True,
									reference_class=self.__class__.__name__)
		self.path=self.folder+self.handle
		return True
		
	@classmethod
	def encode(cls,t,prf):
		"""Encode time, original image sizes, profile points into a single 2D array."""
		if len(prf)!=3:
			return None
		if len(prf[0])!=2:
			return None
		(w,h),x,y=prf
		# Cast double time into eight 8-bit integers
		ta=np.array(binary_cast([t],'d','HHHH'))  
		# Create the concatenated arrays
		x=np.concatenate((ta,x)) # time, then x
		y=np.concatenate(([w,h,0,0],y)) # width, height, two padding zeros, then y
		# Transpose to fit shape (n,2)
		out=np.array([x,y]).transpose()
		return out
			
	@classmethod
	def decode(cls,flattened):
		"""Decodes a flattened 2D array into t, (w,h), x, y structures"""
		if len(flattened)<2:
			return None
		x,y=flattened.transpose()
		t=binary_cast(x[:4],'HHHH','d')[0]
		w=y[0]
		h=y[1]
		return t,((w,h),x[4:],y[4:])
	
	

