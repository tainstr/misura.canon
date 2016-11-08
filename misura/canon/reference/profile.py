# -*- coding: utf-8 -*-
"""Option persistence on HDF files."""
import tables
from ..parameters import cfilter
import numpy as np
from reference import Reference
# TODO: Unify commit/append!!! They are basically the same!
from variable import VariableLength, binary_cast


def decode_time(node, index):
    t = binary_cast(node[index][:4, 0], 'HHHH', 'd')[0]
    return t


class Profile(VariableLength):
    unbound = VariableLength.unbound.copy()
    unbound['decode_time'] = decode_time
    atom = tables.UInt16Atom(shape=(2,))

    def create(self):
        """Create a VLArray for containing frames as variable-length sequences of t,width, pixels..."""
        f = Reference.create(self)
        if not f:
            return False
        self.outfile.create_vlarray(where=f,
                                    name=self.handle,
                                    atom=self.atom,
                                    title=self.name,
                                    filters=cfilter,
                                    createparents=True,
                                    reference_class=self.__class__.__name__)
        self.path = self.folder + self.handle
        return True

    @classmethod
    def encode(cls, t, prf):
        """Encode time, original image sizes, profile points into a single 2D array."""
        if len(prf) != 3:
            return None
        if len(prf[0]) != 2:
            return None
        (w, h), x, y = prf
        # Cast double time into four 16-bit integers
        ta = np.array(binary_cast([t], 'd', 'HHHH'))
        # Create the concatenated arrays
        x = np.concatenate((ta, x))  # time, then x
        # width, height, two padding zeros, then y
        y = np.concatenate(([w, h, 0, 0], y))
        # Transpose to fit shape (n,2)
        out = np.array([x, y]).transpose()
        return out

    @classmethod
    def decode(cls, flattened):
        """Decodes a flattened 2D array into t, (w,h), x, y structures"""
        if len(flattened) < 2:
            return None
        x, y = flattened.transpose()
        t = binary_cast(x[:4], 'HHHH', 'd')[0]
        w = y[0]
        h = y[1]
        return t, ((w, h), x[4:], y[4:])


def accumulate_coords(x,y):
    """Convert absolute coords x, y into cumulative coords relative to the starting values of x,y.
    2 5 8
    1 x 7
    0 3 6"""
    d = np.diff(x)*3 + np.diff(y) +4
    return d.astype('uint8')

def decumulate_coords(x0, y0, v):
    """Convert cumulative coords v into absolute coords x,y"""
    v-=4
    """
    -2  1 4
    -3  x 3
    -4 -1 2"""
    a = np.abs(v)
    # Convert to single coordinate +1,0,-1 movements
    x = (a!=1)*np.sign(v)
    y = (a!=3)*np.sign(v)*(1-2*(a==2))
    # Convert to cumulative values
    x = np.concatenate(([x0], x0+np.cumsum(x)))
    y = np.concatenate(([y0], y0+np.cumsum(y)))
    return x.astype('uint16'), y.astype('uint16')
    
    

class CumulativeProfile(Profile):
    atom = tables.UInt8Atom() 
    
    @classmethod
    def encode(cls, t, prf):
        """Encode time, original image sizes, profile points into a single 2D array."""
        if len(prf) != 3:
            return None
        if len(prf[0]) != 2:
            return None
        (w, h), x, y = prf
        # Cast double time,  16-bits integer dimensions and starting coords into 16 8-bits integers
        coords = np.array(binary_cast([t, w, h, x[0], y[0]], 'dHHHH', '<16B'))
        cumul = accumulate_coords(x,y)
        out = np.concatenate((coords, cumul))
        return out

    @classmethod
    def decode(cls, flattened):
        """Decodes a flattened 2D array into t, (w,h), x, y structures"""
        if len(flattened) < 2:
            return None
        t, w, h, x0, y0 = binary_cast(flattened[:16], '<16B', 'dHHHH')
        x, y = decumulate_coords(x0, y0, flattened[16:])
        return t, ((w, h), x, y)  
        