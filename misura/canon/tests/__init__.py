#!/usr/bin/python
# -*- coding: utf-8 -*-
import os, sys
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