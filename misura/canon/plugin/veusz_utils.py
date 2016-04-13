#!/usr/bin/python
# -*- coding: utf-8 -*-
"""3rd-party Veusz Plugin utilities"""
import collections

veusz_datasetplugins = collections.OrderedDict()
veusz_toolsplugins = collections.OrderedDict()

class VeuszPluginsModuleHook(object):
    
    def __init__(self, plugins):
        self._PluginsHook_plugins = plugins
        self._PluginsHook_protected = dir(self)
    
    def __getattr__(self, name):
        if name.startswith('_PluginsHook_') or name in self._PluginsHook_protected:
            return object.__getattribute__(self, name)
        return getattr(self._PluginsHook_plugins, name)
    
