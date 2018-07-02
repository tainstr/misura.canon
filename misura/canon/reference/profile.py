# -*- coding: utf-8 -*-
"""Option persistence on HDF files."""
import tables
import numpy as np

from ..parameters import cfilter
from .reference import Reference
# TODO: Unify commit/append!!! They are basically the same!
from .variable import VariableLength, binary_cast


def decode_time_uint16(node, index):
    t = binary_cast(node[index][:4, 0], 'HHHH', 'd')[0]
    return t


class Profile(VariableLength):
    unbound = VariableLength.unbound.copy()
    unbound['decode_time'] = decode_time_uint16
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
    def encode(cls, td):
        """Encode time, original image sizes, profile points into a single 2D array."""
        t, prf = td
        if len(prf) != 3:
            return None
        if len(prf[0]) != 2:
            return None
        (w, h), x, y = prf.astype(np.uint16)
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

def explode_jumps(x, y):
    dx = np.diff(x)
    dy = np.diff(y)
    ox = []
    oy = []
    for i, vx in enumerate(dx):
        ax = abs(vx)
        vy = dy[i]
        ay = abs(vy)
        if ax<=1 and ay<=1:
            ox.append(vx)
            oy.append(vy)
            continue
        ox += [0]*max(ax, ay)
        oy += [0]*max(ax, ay)
        
        if ax:
            ox[-ax:] = [np.sign(vx)]*ax
        if ay:
            oy[-ay:] = [np.sign(vy)]*ay
    
    ox, oy = np.array(ox), np.array(oy)
    
    return ox, oy

scale = 9
def couple(d, scale=9):
    """Compact couples of bits into bytes"""
    # Force even array length by appending an identity
    if len(d) % 2 == 1:
        d = np.concatenate((d, [4]))
    #  Sum even with odd by applying a scaling
    d1 = d[0::2] + d[1::2]*scale
    return d1

def decouple(v, scale=9):
    # Conversion to bigger SIGNED int
    v=v.astype(np.uint16)
    # Unpack bits from bytes
    v_even = v % scale
    v_odd = v // scale
    v = np.array([v_even, v_odd]).flatten('F')
    # Cut away padding identity if present
    if v[-1] == 4:
        v=v[:-1]
    return v    

def accumulate_coords(x,y):
    """Convert absolute coords x, y into cumulative coords relative to the starting values of x,y.
    2  5  8
    1 (4) 7
    0  3  6
    4 never exists (means identity)"""
    dx,dy = explode_jumps(x,y)
    d = (dx+1)*3 + (dy+1)
    return couple(d, scale)

def decumulate_coords(x0, y0, v):
    """Convert cumulative coords v into absolute coords x,y"""
    # Conversion to SIGNED int
    v = decouple(v, scale)
    # 4-translation
    v -= 4
    """
    Results in:
    -2  1   4
    -3  (0) 3
    -4  -1  2"""
    a = np.abs(v)
    sgn = np.sign(v)
    # Convert to single coordinate +1,0,-1 movements
    # Any abs>1 contains an x movement equal to the sign of the 4-trans
    # Any abs<=1 contains a 0 x movement
    x = (a>1)*sgn
    # Any abs==3 contains no y movement
    # Otherwise, y movement is equal to the sign(v), unless a==2, 
    # in which case it's the opposite: (1-2*(a==2))
    y = (a!=3)*sgn*(1-2*(a==2))
    # Convert to cumulative values
    x = np.concatenate(([x0], x0+np.cumsum(x)))
    y = np.concatenate(([y0], y0+np.cumsum(y)))
    return x.astype('uint16'), y.astype('uint16')
    
def decode_time_uint8(node, index):
    t = binary_cast(node[index][:8], '<8B', 'd')[0]
    return t   

class CumulativeProfile(Profile):
    unbound = Profile.unbound.copy()
    unbound['decode_time'] = decode_time_uint8
    atom = tables.UInt8Atom() 
    
    @classmethod
    def encode(cls, td):
        """Encode time, original image sizes, profile points into a single 2D array."""
        t, prf = td
        if len(prf) != 3:
            return None
        if len(prf[0]) != 2:
            return None
        (w, h), x, y = prf
        # Cast double time,  16-bits integer dimensions and starting coords into 16 8-bits unsigned integers
        coords = np.array(binary_cast([t, w, h, x[0], y[0]], 'dHHHH', '<16B'))
        cumul = accumulate_coords(x.astype(np.uint8),y.astype(np.uint8))
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
        