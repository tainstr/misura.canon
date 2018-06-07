# -*- coding: utf-8 -*-
"""Utilities for importing data into Misura HDF file format"""
import os
from time import time
from fnmatch import fnmatch
from collections import OrderedDict

import numpy as np

from ..option import ao
from misura.canon.logger import get_module_logging
logging = get_module_logging(__name__)
from .. import reference

def base_dict():
    """Returns a dictionary containing typical options for a legal configurable object"""
    out = OrderedDict()
    ao(out, 'name', 'String', 'Name', name='Name')
    ao(out, 'mro', 'List', name='mro', attr=['Hidden'])
    ao(out, 'comment', 'String', '')
    ao(out, 'preset', 'Preset', '', attr=['Hidden'])
    ao(out, 'dev', 'String', attr=['Hidden'])
    ao(out, 'devpath', 'String', attr=['Hidden'])
    ao(out, 'fullpath', 'String', attr=['Hidden'])
    ao(out, 'zerotime', 'Float', name='Start time', attr=['Hidden'])
    ao(out, 'initInstrument', 'Progress', attr=['Hidden'])
    return out

def measure_dict():
    """Returns a dictionary containing typical options for a generic Measure object"""
    out = base_dict()
    out['name']['current'] = 'Measure'
    ao(out, 'nSamples', 'Integer', 1, readLevel=3)
    ao(out, 'id', 'String', 'Conversion source ID', readLevel=3)
    ao(out, 'uid', 'String', 'Unique ID', readLevel=5)
    ao(out, 'date', 'Date', '00:00:00 01/01/2000', name='Test date')
    ao(out, 'zerotime', 'Float', name='Acquisition starting time', readLevel=4)
    ao(out, 'elapsed', 'Float', name='Test duration', unit='second')
    ao(out, 'operator', 'String', name='Operator')
    return out

def smp_dict():
    """Returns a dictionary containing typical options for a generic Sample object"""
    out = base_dict()
    out['name']['current'] = 'Sample'
    ao(out, 'idx', 'Integer', attr=['Hidden'])
    ao(out, 'ii', 'Integer', attr=['Hidden'])
    ao(out, 'initialDimension', 'Float', 0., name='Initial Dimension')
    return out
    
def kiln_dict():
    """Returns a dictionary containing typical options for Kiln object"""
    out = base_dict()
    out['name']['current'] = 'Kiln'
    ao(out, 'serial', 'String', readLevel=4)
    ao(out, 'curve', 'Hidden', [[0, 0]], 'Heating curve')
    ao(out, 'thermalCycle', 'ThermalCycle', 'default')
    ao(out, 'T', 'Float', 0, 'Temperature', unit='celsius')
    ao(out, 'P', 'Float', 0, 'Power', unit='percent')
    ao(out, 'S', 'Float', 0, 'Setpoint', unit='celsius')
    ao(out, 'maxHeatingRate', 'Float', 0, 'Max Heating Rate', readLevel=3)
    ao(out, 'maxControlTemp', 'Float', 0, 'Max Control Temp', readLevel=3)
    ao(out, 'minControlTemp', 'Float', 0, 'Min Control Temp', readLevel=3)
    ao(out, 'maxElementTemp', 'Float', 0, 'Max Element Temp', readLevel=3)
    ao(out, 'minElementTemp', 'Float', 0, 'Min Element Temp', readLevel=3)
    ao(out, 'ksn', 'String', readLevel=4)
    return out

def instr_dict():
    """Returns a dictionary containing typical options for generic instrument object"""
    out = base_dict()
    ao(out, 'nSamples', 'Integer', 1, 'Number of samples', readLevel=3)
    ao(out, 'devices', 'List', attr=['Hidden'])
    ao(out, 'initTest', 'Progress', attr=['Hidden'])
    ao(out, 'closingTest', 'Progress', attr=['Hidden'])
    return out

def server_dict():
    out = base_dict()
    out['name']['current'] = 'server'
    ao(out, 'name', 'String', 'server')
    ao(out, 'isRunning', 'Boolean', False, readLevel=4)
    ao(out, 'runningInstrument', 'String')
    ao(out, 'lastInstrument', 'String')
    ao(out, 'log', 'Log')
    ao(out, 'eq_sn', 'String', readLevel=4)
    return out


def smp_tree():
    """Tree for generical sample"""
    return  OrderedDict({'self': smp_dict()})

def instr_tree(): 
    """Tree for generical instrument"""
    return OrderedDict({'self': instr_dict(),
                'measure': {'self': measure_dict()}})

def tree_dict(): 
    """Main tree"""
    return OrderedDict({'self': server_dict(),
            'kiln': {'self': kiln_dict()}})


def create_tree(outFile, tree, path='/'):
    """Recursive tree structure creation"""
    for key, foo in tree.list():
        if outFile.has_node(path, key):
            logging.debug('Path already found:', path, key)
            continue
        logging.debug('Creating group:', path, key)
        outFile.create_group(path, key, key)
        dest = path + key + '/'
        if outFile.has_node(dest):
            continue
        create_tree(outFile, tree.child(key), dest)
        
class Converter(object):
    name = 'Base Converter'
    file_pattern = '*'
    pid = 'Data conversion'
    confdb = {}
    
    def __init__(self):
        self.outpath = ''
        self.interrupt = False
        self.outFile = False
        self.log_ref = False
        self.conversion_start_time = time()
        
    def post_open_file(self, navigator, *a, **k):
        return False
        
    def log(self, *msg, **kw):
        # TODO: check zerotime
        priority = kw.get('priority', 10)
        msg = logging.info(*msg)
        # Create log reference if missing
        if not self.log_ref and self.outFile:
            log_opt = ao({}, 'log', 'Log')['log']
            self.log_ref = reference.Log(self.outFile, '/', log_opt)
        # Append to log reference
        if self.log_ref:
            t = time() - self.conversion_start_time
            self.log_ref.commit([[t, (priority, msg)]])
    
    def cancel(self):
        """Interrupt conversion and remove output file"""
        if self.outFile:
            self.outFile.close()
        if os.path.exists(self.outpath):
            os.remove(self.outpath)
     
    def convert(self, *a, **kw):
        """Override this to do the real conversion"""
        assert False,'Unimplemented'
        
def create_dataset(outFile, node_path, opt, data, timecol=None, cls=reference.Array):
    """Create on outFile, in group node_path, a dataset for option `opt`.
    Writes `timecol` and `data`. """
    if cls==reference.FixedTimeArray:
        opt['t0'] = timecol[0]
        opt['dt'] = timecol[1]
        
    ref = cls(
        outFile, node_path, opt)
    # Recreate the reference so the data is clean
    ref.dump()
    path = ref.path
    base_path = path[8:]
    # Create hard links
    if not outFile.has_node(base_path):
        outFile.link(base_path, path)
    if cls==reference.Array:
        data = np.array([timecol[:len(data)], data]).transpose()
    elif cls==reference.FixedTimeArray:
        data = np.array(data).transpose()
    ref.append(data)
    return ref

data_importers = set([])
        
def search_registry(filename):
    """Find a matching converter for filename"""
    for converter in data_importers:
        for pattern in converter.file_pattern.split(';'):
            if fnmatch(filename, pattern):
                logging.debug('Found converter', filename, converter.file_pattern, pattern)
                return converter
    logging.error('No converter found', filename)
    return False

def get_converter(path):
    converter_class = search_registry(path)
    if not converter_class:
        return False
    return converter_class()
    
def convert_file(path, *args, **kwargs):
    """Do the conversion"""
    converter = get_converter(path)
    outpath = converter.convert(path, *args, **kwargs)
    return outpath

class NullTasks(object):
    jobs = lambda *a, **k: 0
    job = lambda *a, **k: 0
    done = lambda *a, **k: 0

        