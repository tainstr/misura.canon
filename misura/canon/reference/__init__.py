# -*- coding: utf-8 -*-
"""Option persistence on HDF files."""
from reference import Reference
from array import Array, Boolean, Rect, Meta,  Point
from log import Log
from profile import Profile
from binary import Binary
from image import Image, ImageM3, ImageBMP
from obj import Object
from variable import VariableLength, binary_cast
from traceback import print_exc


def get_reference(opt):
    """Returns a suitable Reference subclass for the option `opt`."""
# 	if 'History' not in opt['attr']:
# 		return False
    t = opt['type']
    if t in ['Float', 'Integer', 'Progress', 'Time']:
        return Array
    if t in ['Binary', 'String', 'TextArea', 'Image']:
        return Binary
# 	if t=='Image':
# 		return Image
    if t == 'Profile':
        return Profile
    if t == 'Rect':
        return Rect
    if t == 'Point':
        return Point
    if t == 'Meta':
        return Meta
    if t == 'Log':
        return Log
    if t == 'Boolean':
        return Boolean
    # if not suitable type is found, return object
    return Object


def get_node_reference_class(outfile, path):

    name = outfile.has_node_attr(path, '_reference_class')
    if name is False:
        print 'No _reference_class attribute for', path
        return False
    name = outfile.get_node_attr(path, '_reference_class')
    g = globals()
    cls = g.get(name, False)
    if cls is False:
        print 'No class for _reference_class', name, path
        return False
    return cls


def get_node_reference(outfile, path):
    """Return a reference object built from a node located in `path` on SharedFile `outfile`"""
    cls = get_node_reference_class(outfile, path)
    if cls is False:
        return False
    ref = cls(outfile, path)
    return ref


def db_copy(fromdb, output_path, start=-1, end=-1):
    """Copy from fromdb onto `output` SharedFile instance all references and structures, ranging from time `start` to time `end`.
    """
    print 'db_copy'
    fromdb.flush()
    print 'flushed'
# 		self.reopen()
# 		print 'reopened'
    stats = False
    print 'getting header'
    h = fromdb.header(False)
    print 'got', h
    for path in h:
        try:
            print 'starting copy', path, start, end, output_path
            ref = get_node_reference(fromdb, path)
            stats = ref.copy(output_path, start, end, stats)
        except:
            print 'db_copy error', path, print_exc()
            continue
    print 'db_copy() DONE', stats
    return stats
