# -*- coding: utf-8 -*-
"""Option persistence on HDF files."""
import numpy as np
import tables

from ..parameters import cfilter
from .reference import Reference
from .variable import VariableLength, binary_cast


def decode_time(node, index):
    t = binary_cast(node[index][:8], 'BBBBBBBB', 'd')[0]
    return t


class Binary(VariableLength):

    """Storage reference for Binary types. 
    Binary data is converted into integer sequences and stored in a VLArray.
    """
    n = 0
    """Current counter number"""
    unbound = VariableLength.unbound.copy()
    unbound['decode_time'] = decode_time

    def create(self):
        """Create the output array"""
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
    def encode(cls, t, data):
        if data is False:
            return False
        ta = binary_cast([t], 'd', 'BBBBBBBB')
        dat = np.array([ord(d) for d in data]).astype('i8')
        return np.concatenate((ta, dat))

    @classmethod
    def decode(cls, flattened):
        if len(flattened) < 8:
            return None
        t = binary_cast(flattened[:8], 'BBBBBBBB', 'd')[0]
        dat = ''.join([chr(d) for d in flattened[8:]])
        return t, dat
