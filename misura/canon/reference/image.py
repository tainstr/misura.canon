# -*- coding: utf-8 -*-
"""Option persistence on HDF files."""
import tables
from ..parameters import cfilter
import numpy as np
from reference import Reference
from variable import VariableLength, binary_cast
import binary
import cPickle as pickle


def decode_time(node, index):
    t = binary_cast(node[index][:8], 'BBBBBBBB', 'd')[0]
    return t


class Image(VariableLength):

    unbound = VariableLength.unbound.copy()
    unbound['decode_time'] = decode_time

    def create(self):
        """Create a VLArray for containing frames as variable-length sequences of t,width, pixels..."""
        f = Reference.create(self)
        if not f:
            return False
        self.outfile.create_vlarray(where=f,
                                    name=self.handle,
                                    atom=tables.UInt8Atom(shape=()),
                                    title=self.name,
                                    filters=cfilter,
                                    createparents=True,
                                    reference_class=self.__class__.__name__)
        self.path = self.folder + self.handle
        return True

    @classmethod
    def compress(cls, img, as_string=False):
        """Simple lossless image array compression. Flatten, zlib."""
        h0, w0 = img.shape
        w = binary_cast([w0], 'H', 'BB')
        h = binary_cast([h0], 'H', 'BB')
        cp = np.concatenate((w, h, img.astype('uint8').flatten()))
        # VLR.cmp: more 2x compression
        scp = VariableLength.compress(cp)
        print 'compressed', w0, h0, len(pickle.dumps(img)), len(pickle.dumps(scp))
        if as_string:
            return scp
        # translate string into unit8 for storage
        vcp = np.array(map(ord, scp)).astype('uint8')
        return vcp

    @classmethod
    def decompress(cls, imgz):
        """Decompress lossless image data back into 2D array."""
        # translate back uint8 into string
        if not isinstance(imgz, str):
            imgz = ''.join(map(chr, imgz))
        # zlib decompression
        imgz = VariableLength.decompress(imgz)
        ####
        w = binary_cast(imgz[:2], 'BB', 'H')[0]
        h = binary_cast(imgz[2:4], 'BB', 'H')[0]
        img = imgz[4:]
        print 'decompressing', w, h, len(img)
        img = np.reshape(img, (h, w))
# 		print 'Decompression',len(imgz),len(img.flatten())
        return img

    @classmethod
    def encode(cls, t, img):
        """Encode time, image into a single array containing time, width, height, and flattened pixel intensities."""
        # Cast double time into eight 8-bit integers
#		ta=np.array(binary_cast([t],'d','hhhh'))
        ta = np.array(binary_cast([t], 'd', 'BBBBBBBB'))
        # Cast w,h 16-bit unsigned integers into two unsigned 8-bit integers
        cp = cls.compress(img)
        out = np.concatenate((ta, cp)).astype('B')
        return out

    @classmethod
    def decode(cls, flattened):
        """Decodes a flattended 1D array into t, img structures"""
        if len(flattened) < 8:
            return None
        t = binary_cast(flattened[:8], 'BBBBBBBB', 'd')[0]
        img = cls.decompress(flattened[8:])
        return t, img
        
class ImageM3(binary.Binary):
    """Misura3 image format"""
    pass

class ImageBMP(binary.Binary):
    """Bitmap image format"""
    pass
