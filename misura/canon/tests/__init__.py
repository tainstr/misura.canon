#!/usr/bin/python
# -*- coding: utf-8 -*-
import os, sys
import numpy as np

def determine_path(root=__file__):
    """Borrowed from wxglade.py"""
    try:
        #       root = __file__
        if os.path.islink(root):
            root = os.path.realpath(root)
        return os.path.dirname(os.path.abspath(root))
    except:
        print "I'm sorry, but something is wrong."
        print "There is no __file__ variable. Please contact the author."
        sys.exit()

testdir = determine_path()+'/'  # Executable path

def verify_point(test, env, time=None, temp=None, value=None, comment=''):
    """Verify that Script output is equal to passed parameters"""
    if time in ['None', None]:
        test.assertEqual(env.time, None)
    else:
        test.assertAlmostEqual(env.time, time)
    if temp in ['None', None]:
        test.assertEqual(env.temp, None)
    else:
        test.assertAlmostEqual(env.temp, temp, delta=0.01)
    if value in ['None', None]:
        test.assertEqual(env.value, None)
    else:
        test.assertAlmostEqual(env.value, value)
    test.assertEqual(env.comment, comment)
    

from misura.canon import logger
class DummyInstrument(dict):
    measure = {}
    kiln = {}
    running = True
    log = logger.Log
    _parent = None
    
    def __init__(self, fullpath=False):
        super(DummyInstrument, self).__init__()
        if not fullpath:
            return
        fp = fullpath.split('/')
        dp = fp[-1]
        if dp == '':
            self['devpath'] = 'MAINSERVER'
            self['fullpath'] = '/'
        else:
            self['devpath'] = dp
            self['fullpath'] = '/'.join(fp)  
    
    def parent(self):
        return self._parent
    
    def set(self, key, val):
        self[key] = val

    def stop_acquisition(self):
        self.running = False
        
class FakeStorageFile(object):

    """Faking an hdf file"""
    r = range(100)
    r += range(100, 0, -1)
    nrows = len(r)
    r = np.array(r) * 1.
    t = np.arange(nrows) * 15.
    T = r * 10

    h = np.concatenate((np.linspace(90, 95, 50),
                        np.linspace(95, 6, 150)))
    cohe = np.concatenate((np.linspace(70, 98, 100),
                           np.linspace(97, 31, 100)))

    w = np.concatenate((np.linspace(60, 50, 120),
                        np.linspace(50, 180, 80)))
    Left_pos = np.linspace(0, 5, 200)
    Right_pos = Left_pos
    dil = Left_pos + Right_pos

    def __init__(self):
        self.nodes = {'/hsm/sample0/h': self.t_arr(self.h),
                      '/hsm/sample0/cohe': self.t_arr(self.cohe),
                      '/hsm/sample0/w': self.t_arr(self.w),
                      '/hsm/sample0/dil': self.t_arr(self.dil),
                      '/kiln/T': self.t_arr(self.T)}

    def t_arr(self, arr):
        return np.array([self.t, arr]).transpose()

    def get_node(self, path):
        return self.nodes[path]

    def close(self):
        return True

    def set_time_limit(self, *a, **k):
        return True

    def set_limit(self, *a, **k):
        return True

    def min(self, curve):
        c = self.get_node(curve)[:, 1]
        return 0, 0, min(c)

    def max(self, curve):
        c = self.get_node(curve)[:, 1]
        return 0, 0, max(c)
    

def checkCompile(test, si, out):
    """Check if Script is compiled correctly"""
    for k, opt in si.describe().iteritems():
        if opt['type'] != 'Script':
            continue
        test.assertTrue(si.all_scripts.has_key(k), 'Missing Script ' + k)
        si.all_scripts[k].eval(out, si)
        outopt = False
        # Find output option (child)
        for handle, desc in out.describe().iteritems():
            if desc['parent'] == k:
                outopt = handle
                break
        if not outopt:
            return
        o = out[outopt]
        print 'checkCompile', k, repr(o)
        t = None if o['time'] == 'None' else o['time']
        T = None if o['temp'] == 'None' else o['temp']
        v = None if o['value'] == 'None' else o['value']

        verify_point(test, si.env, o['time'], o['temp'], o['value'])
