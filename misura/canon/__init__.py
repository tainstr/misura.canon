#!/usr/bin/python
# -*- coding: utf-8 -*-
"""Misura shared utilities"""
# Sub modules
from . import csutil
from . import bitmap
from . import circle

# Sub packages
from . import indexer
from . import milang
from . import option
from . import reference
from . import plugin


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