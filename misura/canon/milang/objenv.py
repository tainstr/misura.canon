#!/usr/bin/python
# -*- coding: utf-8 -*-
"""Misura Language or Mini Language. 
Secure minimal Python language subset for conditional evaluation of numerical datasets."""

import exceptions

from dataenv import DataEnvironment


class InterfaceEnvironment(DataEnvironment):

    """Interface object providing access to additional options"""
    whitelist = DataEnvironment.whitelist + ['time']
    _obj = {}

    def __init__(self, obj=False):
        DataEnvironment.__init__(self)
        if obj:
            self.obj = obj

    @property
    def obj(self):
        return self._obj

    @obj.setter
    def obj(self, obj):
        """Set the object towards which the environment acts as an inteface. 
        Set the prefix according to obj fullpath."""
        self._obj = obj
        if obj in [None, False]:
            return
        if obj.has_key('fullpath'):
            # Use get() and not [] because it can be an autoproxy
            self.prefix = obj.get('fullpath')
# 			obj.log.debug('object fullpath set to',self.prefix)
        else:
            obj.log.debug('object fullpath not found!', obj)

    def Log(self, s):
        """Log a message both on MiLang and on the interfaced object"""
        DataEnvironment.Log(self, s)
        log = getattr(self.obj, 'log', False)
        if log:
            self.obj.log(s)

    def Opt(self, name):
        """Returns any option by its `name`"""
        return self.obj.get(name)

    def Meta(self, name):
        """Returns meta value for script option `name`"""
#		print self.obj.keys()
        return self.obj.get('Meta_' + name)

    def MetaPart(self, name, part):
        """Returns `part` item of a `name` script option dictionary (eq. to Opt('name')['part']"""
        r = self.Meta(name)[part]
        if r == 'None':
            return None
        return r

    def MetaValue(self, name): return self.MetaPart(name, 'value')

    def MetaTime(self, name): return self.MetaPart(name, 'time')

    def MetaTemp(self, name): return self.MetaPart(name, 'temp')

    def Leaf(self, name):
        """Returns a leaf interface object"""
        r = self.obj.child(name)
        if r is None:
            raise exceptions.BaseException(
                'Child object does not exist: ' + name)
        # Re-instantiate any class inheriting InterfaceEnvironment
        ie = self.__class__()
        ie.obj = r
        return ie


class InstrumentEnvironment(InterfaceEnvironment):

    def stop_acquisition(self, save=True):
        # Signal acq stop. We are in different process and cannot directly call
        # stop_acquisition!
        self.obj.log.info(
            'Script is requesting acquisition end. ' + self.handle)
        self.obj.root.set('isRunning', False)


class KilnEnvironment(InterfaceEnvironment):
    whitelist = InterfaceEnvironment.whitelist + ['stop_heating',
                                                  'stop_cycle',
                                                  'skip_current',
                                                  'skip_to',
                                                  'start_parametric_heating',
                                                  'stop_parametric_heating']
