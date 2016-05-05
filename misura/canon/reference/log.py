# -*- coding: utf-8 -*-
"""Option persistence on HDF files."""
from ..parameters import cfilter
from reference import Reference
# TODO: Unify commit/append!!! They are basically the same!
import numpy as np


class Log(Reference):
    fields = [('t', 'float64'), ('priority', 'uint8'), ('msg', 'S10000')]

    def create(self):
        """Create a Table instance configured for Log storage"""
        f = Reference.create(self)
        if not f:
            return False

# print 'creating
# table',self.outfile,self.path,f,self.handle,self.name,self.outfile.get_path()
        self.outfile.create_table(where=f,
                                  name=self.handle,
                                  description=np.dtype(self.fields),
                                  title=self.name,
                                  filters=cfilter, createparents=True,
                                  reference_class=self.__class__.__name__)
# 		print 'setting path'
        self.path = self.folder + self.handle
# 		print 'done',self.path
        self.outfile.flush()
        return True

    @classmethod
    def encode(cls, t, dat):
        if len(dat) != 2:
            print 'Log: wrong data length'
            return None
        dat = list(dat)
        if isinstance(dat[1], unicode):
            dat[1] = dat[1].encode('ascii', 'replace')
        if not isinstance(dat[1], str):
            return None
        return np.array([tuple([t] + dat)], dtype=cls.fields)

    def interpolate(self, *a, **k):
        """Interpolation has no sense for logs."""
        return True
