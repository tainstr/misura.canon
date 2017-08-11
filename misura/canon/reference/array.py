# -*- coding: utf-8 -*-
"""Option persistence on HDF files."""
import numpy as np

from ..parameters import cfilter
from .reference import Reference


class Array(Reference):
    fields = [('t', 'float64'), ('v', 'float64')]

    def __init__(self, outfile, folder=False, opt=False, write_current=False, with_summary=True):
        self.with_summary = with_summary
        Reference.__init__(self, outfile, folder=folder,
                           opt=opt, write_current=write_current)

    def create(self, fields=False):
        """Create an EArray (enlargeable array) as data storage"""
        f = Reference.create(self)
        if not f:
            return False
        if fields:
            self.fields = fields
        self.outfile.create_table(where=f,
                                  name=self.handle,
                                  description=np.dtype(self.fields),
                                  title=self.name,
                                  filters=cfilter,
                                  createparents=True,
                                  reference_class=self.__class__.__name__)
        self.path = self.folder + self.handle
        self.outfile.flush()
        # Create the summary mirror
        ver = self.outfile.get_version()
        if (not self.path.startswith(ver+'/summary/')) and len(self.fields) == 2 and self.with_summary:
            dest = ver+'/summary'+self.folder[len(ver):]
            print('Creating Array summary mirror', dest)
            self.summary = Array(
                self.outfile, dest, opt=self.opt)
        return True

    def open(self, folder):
        """Open an existing Array in `folder` with its summary Array"""
        Reference.open(self, folder)
        # Open the summary mirror
        if not self.path.startswith('/summary') and len(self.fields) == 2:
            self.summary = Array(self.outfile, '/summary' + self.path)

    @classmethod
    def encode(cls, dat):
        if len(cls.fields) == 1:
            return np.array(dat, dtype=cls.fields)
        
        t, dat = dat
        if len(cls.fields) == 2:
            return np.array([(t, dat)], dtype=cls.fields)
        else:
            dat = list(dat)
            return np.array([tuple([t] + dat)], dtype=cls.fields)

    @classmethod
    def decode(cls, dat):
        if len(dat) == 1:
            dat = dat[0]
        if len(cls.fields)==1:
            return Reference.decode((dat,))
        n = len(dat)
        if n != len(cls.fields):
            return None
        if n == 2:
            return list(dat)
        return Reference.decode(tuple(dat))

    def interpolate(self, step=1, kind=1):
        """Array interpolation for summary synchronization."""
        vt = Reference.interpolate(self, step)
        if vt is False:
            return False
        # Value sequence
        # starting from the oldest time minus step
        oldi = self.get_time(vt[0] - step)
        # Check if we have enough points to interpolate
        if len(self) - oldi < 5:
            return False
        # If possible, go back one more point, for interpolation safety
        if oldi > 1:
            oldi -= 1
        # Decode values and separate time and value vectors
        dat = self[oldi:]
        dat = np.array(dat)
        dat = dat.transpose()
        # Build a linear spline using vt points as knots
        #f=LSQUnivariateSpline(dat[0],dat[1],vt, k=kind)
        # Do a linear fitting
        (slope, const), res, rank, sing, rcond = np.polyfit(
            dat[0], dat[1], kind, full=True)
        # Build a vectorized evaluator
        f = np.vectorize(lambda x: slope * x + const)
        while vt[0] < dat[0][0] and len(vt) > 1:
            vt = vt[1:]
        while vt[-1] > dat[0][-1] and len(vt) > 1:
            vt = vt[:-1]
        if len(vt) <= 1:
            return False
        try:
            # Interpret time series
            out = f(vt)
        except:
            print('Array.interpolate', self.path, vt, dat)
            raise
        # Encode in (t,v) append-able list
        out = np.array([vt, out]).transpose()
        self.summary.commit(out)
        return True


class FixedTimeArray(Array):
    """An array without a time column"""
    fields = [('v', 'float64')]
    
    def __getitem__(self, idx_or_slice):
        t0 = self.opt['t0']
        dt = self.opt['dt']
        if isinstance(idx_or_slice, int):
            return [t0+idx_or_slice*dt, self.decode(self.outfile.col_at(self.path, idx_or_slice, raw=True))]
        s = idx_or_slice.start or 0
        t0 = t0+s*dt
        return [[t0+i*dt, self.decode(d)] for i, d in enumerate(self.outfile.col(self.path, idx_or_slice, raw=True))]

    def time_at(self, idx=-1):
        """Returns the time label associated with the last committed point"""
        if idx<0:
            idx = len(self)+idx
        return self.opt['t0']+self.opt['dt']*idx

    def get_time(self, t):
        """Finds the nearest row associated with time `t`"""
        return int((t-self.opt['t0'])/self.opt['dt'])


class Boolean(Array):

    """A True/False value"""
    fields = [('t', 'float64'), ('v', 'uint8')]


class Rect(Array):

    """An Array with 5 columns, one for the time,
    4 for the coordinates of a rectangle"""
    fields = [('t', 'float64'), ('x', 'uint16'),
              ('y', 'uint16'), ('w', 'uint16'), ('h', 'uint16')]


class Point(Array):

    """An Array with 3 columns, one for the time,
    2 for x,y integers"""
    fields = [('t', 'float64'), ('x', 'uint16'), ('y', 'uint16')]


class Meta(Array):

    """An Array reference with 4 columns, one for the time,
    3 for value,time,temp keys of a Meta option type"""
    fields = [('t', 'float64'), ('value', 'float64'),
              ('time', 'float64'), ('temp', 'float64')]

    @classmethod
    def encode(cls, dat):
        """Flatten the Meta dictionary into a float list of t,value,time,temp"""
        t, dat = dat
        if len(dat) > 3:
            print('wrong meta', dat, len(dat))
            return None
        r = (t, dat['value'], dat['time'], dat['temp'])
        return np.array([r], dtype=cls.fields)

    @classmethod
    def decode(cls, dat):
        """Rebuild the Meta dictionary"""
        if len(dat) == 1:
            dat = dat[0]
        if len(dat) != len(cls.fields):
            return None
        return [dat[0], {'value': dat[1], 'time':dat[2], 'temp':dat[3]}]
