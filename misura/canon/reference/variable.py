# -*- coding: utf-8 -*-
"""Option persistence on HDF files."""
import numpy as np
import struct
import zlib
import StringIO
from reference import Reference
#TODO: Unify commit/append!!! They are basically the same!

def binary_cast(data, input_fmt, output_fmt):
	"""Transform the input data from input binary format to output binary format"""
	# Pack in binary format
	p=struct.pack(input_fmt,*data)
	# Unpack into new format
	return struct.unpack(output_fmt,p)
	
class VariableLength(Reference):
	"""Each point array is encoded into a single array, then appended to a variable length array."""
	@classmethod
	def compress(cls,img,level=6):
		"""Compress the data array into a gzip string"""
		g=StringIO.StringIO()
		np.save(g,img)
		g.seek(0)
		raw=g.read()
		return zlib.compress(raw,level)
	
	@classmethod
	def decompress(cls,imgz):
		"""De-compress the data array from a gzip string"""
		# Write the compressed data into the SIO
		img=zlib.decompress(imgz)
		s=StringIO.StringIO()
		s.write(img)
		s.seek(0)
		# Load the gzip file
		return np.load(s)		
	
	def commit(self,data):
		"""Encode data and write it onto the reference node."""
# 		print 'committing',self.path,data
		# Cut too old points
		n=0
		for d in data:
			if d is False: continue
			t,dat=d
			app=self.encode(t,dat)
			if app is None:
				continue
			self.append(app)
			n+=1
		return n	
	
	
		