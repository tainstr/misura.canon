# -*- coding: utf-8 -*-
"""Option persistence on HDF files."""
from binary import Binary
from cPickle import dumps, loads, HIGHEST_PROTOCOL


class Object(Binary):

    @classmethod
    def encode(cls, t, dat):
        dat = dumps(dat, HIGHEST_PROTOCOL)
        return Binary.encode(t, dat)

    @classmethod
    def decode(cls, dat):
        t, obj = Binary.decode(dat)
        obj = loads(obj)
        return obj
