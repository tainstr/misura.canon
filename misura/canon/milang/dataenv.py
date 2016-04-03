#!/usr/bin/python
# -*- coding: utf-8 -*-
"""Misura Language or Mini Language. 
Secure minimal Python language subset for conditional evaluation of numerical datasets."""
from env import BaseEnvironment
import numpy as np
from scipy.interpolate import UnivariateSpline
from .. import csutil


class DataEnvironment(BaseEnvironment):
    # relativi agli oggetti array numpy
    whitelist = BaseEnvironment.whitelist + ['std', 'get', 'set']
    _hdf = False
    """SharedFile instance"""
    prefix = ''
    """Prefix to be applied to all curves. E.g.: /hsm/sample0/"""
    limit = (0, -1)
    """Current limit selection expressed as time limits"""
    lslice = slice(None, None)
    """Last applied slicing interval"""
    spline_cache = {}
    """Approximating splines cache"""
    _temperature_path = False

    @property
    def hdf(self):
        """Trying to anyway access undefined hdf property will raise a exception"""
        if self._hdf is False:
            self.Exit('No data file defined in this environment!')
        return self._hdf

    @hdf.setter
    def hdf(self, f):
        self._hdf = f

    def _cname(self, curve):
        """Retrieve curve name by adding prefix, etc. Activate time limits if needed."""
        if not isinstance(curve, str):
            return False
        # curve is Temperature 
        if curve in ['T']:
            # but unknown if a local T dataset is available in prefix
            if not self._temperature_path:
                curve = self.prefix + 'T'
                if not self.hdf.has_node(curve):
                    curve = '/kiln/T'
                self._temperature_path = curve
                print '########### temperature path = ', curve, self.prefix
            # Take defined temperature path
            curve = self._temperature_path
        else:
            curve = self.prefix + curve
        # Activate time limits for current curve
        self.hdf.set_limit(curve)
        print '_cname returning', curve
        return curve

    def _c(self, curve0):
        """Returns the array object located at prefix+`curve` on the hdf file.
        The curve is already sliced.
        If `curve` is not a string, returns the unchanged object."""
        curve = self._cname(curve0)
        if curve is False:
            return curve0
        print 'getting column', curve
        c = self.hdf.col(curve)
        print 'got column', curve, len(c)
        return c
    Curve = _c

    def xy(self, curve):
        """Separately return x and y arrays as tuples"""
        n = self._c(curve)
        print 'xy', type(n)
        return n[:, 0], n[:, 1]

    def Point(self, t=None, curve=False, idx=None):
        """Set output time and temperature at time  `t` of `curve`"""
        if t == None:
            pt = self.AtIndex('T', idx)
            if pt == None:
                return
            t = pt[0]
        else:
            pt = self.AtTime('T', t)
        self.t(pt[0])
        self.T(pt[1])
        if curve is False:
            return
        val = self.At(curve, t)
        self.Value(val)

    def Spline(self, curve0):
        """Returns an UnivariateSpline object from a t,val dataset.
        `curve` is a dataset path, prefix excluded, or a dataset alias."""
        spline = False
        curve = self._cname(curve0)
        if curve:
            spline = self.spline_cache.get(curve, False)
        if spline is not False:
            return spline
        x, y = self.xy(curve)
        if len(x) == 0:
            self.Exit("Zero-length curve requested " + curve)
        spline = UnivariateSpline(x, y)
        return spline

    def ValueWhere(self, cond, values):
        """Returns the first truth value of cond"""
        i = self.Where(cond)
        if i < 0:
            return -1
        return values[i]

    def _x(self, curve):
        """Retrieve the `x` array constituting the UnivariateSpline of curve"""
        return self._c(curve)[:, 0]
    _t = _x

    def _y(self, curve):
        return self._c(curve)[:, 1]

    def _reset(self):
        BaseEnvironment._reset(self)
        self.limit = (0, -1)
        if self._hdf:
            self.hdf.set_time_limit(*self.limit)
        self.spline_cache = {}

    def Select(self, start=0, end=-1):
        """Limit every function between time start and time end"""
        self.limit = (start, end)
        self.hdf.set_time_limit(start, end)
        print 'Activated limit', self.limit
        return True

    def Start(self):
        """Returns the start of the selection"""
        return self.limit[0]

    def End(self):
        """Returns the end of the selection"""
        return self.limit[1]

    def SelectCooling(self):
        """Set selection to the cooling part of the test"""
        # TODO: verificare che dopo questo massimo, non torni a scendere e a
        # salire
        idx, t, T = self.hdf.max('/kiln/T')
        if idx + 1 >= self.hdf.len('/kiln/T'):
            return False
        self.limit = [t, -1]
        self.hdf.set_time_limit(t, -1)
        return True

    def AtTime(self, curve0, t):
        """t,val of curve at nearest time `t`."""
        curve = self._cname(curve0)
        if curve is False:
            idx = csutil.find_nearest_val(curve0[:, 0], t)
            return curve0[idx]
        idx = self.hdf.get_time(curve, t)
        return self.hdf.col(curve, idx)

    def AtIndex(self, curve0, idx):
        """t,val of curve at index `idx`."""
        curve = self._cname(curve0)
        if curve is False:
            return curve0[idx]
        return self.hdf.col(curve, idx)

    def At(self, curve, t):
        """Value of curve at nearest time `t`."""
        p = self.AtTime(curve, t)
        return p[1]

    def SetCurve(self, curve, t, val):
        """Sets the value of `curve` at the specified time `t` to `val`"""
        # TODO: SetCurve
        pass

    def minmax(self, curve0, op=1):
        """Search global minimum/maximum value of curve."""
        curve = self._cname(curve0)
        if curve is False:  # curve is array
            ops = (min, max)
            #FIXME: time limits are not applied in this case!
            return ops[op](curve0)
        else:  # curve is an hdf path
            ops = (self.hdf.min, self.hdf.max)
            idx, t, v = ops[op](curve)
            return idx, t, v

    def Max(self, curve):
        """Global maximum value of curve."""
        return self.minmax(curve, 1)

    def Min(self, curve):
        """Global minimum value of curve"""
        return self.minmax(curve, 0)

    def Ini(self, curve):
        """Initial value of curve"""
        c = self._cname(curve)
        if c is False:
            return curve[0]
        limit = self.hdf.get_limit(c)
        return self.hdf.col(c, limit.start)

    def Equals(self, curve0, val):
        """Returns time of curve where its value is val"""
        curve = self._cname(curve0)
        if curve is not False:
            return self.hdf.equals(curve, val)
        # TODO: array was passed

    def Nearest(self, curve0, val):
        curve = self._cname(curve0)
        if curve is not False:
            return self.hdf.nearest(curve, val)
        # TODO: array was passed

    def Drops(self, curve0, val):
        """Returns time where curve value drops below val"""
        curve = self._cname(curve0)
        if curve is not False:
            return self.hdf.drops(curve, val)
        # TODO: array was passed

    def Raises(self, curve0, val):
        """Returns time where curve value raises above val"""
        curve = self._cname(curve0)
        if curve is not False:
            return self.hdf.rises(curve, val)
        # TODO: array was passed

    def Len(self, curve0):
        """Length of the curve, according to the current selection"""
        curve = self._cname(curve0)
        if curve is not False:
            return self.hdf.len(curve)
        return len(curve)

    def TimeDerivative(self, curve):
        """Numerical derivative of curve with respect to its time"""
        x, y = self.xy(curve)
        if len(x) < 2:
            return np.array([])
        r = np.gradient(y) / np.gradient(x)
        print 'TimeDerivative', curve, len(r)
        return r

    def Coefficient(self, curve, T0, T1):
        # FIXME
        spline = self.Spline(curve)
        if self.Max('T') < T1:
            self.Exit()
        t0 = self.Nearest('T', T0)
        t1 = self.Nearest('T', T1)
        num = spline(t1) - spline(t0)
        denom = t1 - t0
        return num / denom

    def Ratio(self, numerator, denominator):
        n = self._y(numerator)
        d = self._y(denominator)
        return n / d
