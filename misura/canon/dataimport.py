# -*- coding: utf-8 -*-
"""Utilities for importing data into Misura HDF file format"""
import os
from fnmatch import fnmatch
from misura.canon.option import ao
from misura.canon.logger import Log as logging

registry = []

def base_dict():
    """Returns a dictionary containing typical options for a legal configurable object"""
    out = {}
    ao(out, 'name', 'String', 'Name', name='Name')
    ao(out, 'mro', 'List', name='mro', attr=['Hidden'])
    ao(out, 'comment', 'String', '')
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
    ao(out, 'nSamples', 'Integer', 1, attr=['Hidden'])
    ao(out, 'id', 'String', 'Conversion source ID')
    ao(out, 'uid', 'String', 'Unique ID')
    ao(out, 'date', 'Date', '00:00:00 01/01/2000', name='Test date')
    ao(out, 'elapsed', 'Float', name='Test duration', unit='second')
    ao(out, 'operator', 'String', 'Operator')
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
    ao(out, 'serial', 'String')
    ao(out, 'curve', 'Hidden', [[0, 0]], 'Heating curve')
    ao(out, 'thermalCycle', 'ThermalCycle', 'default')
    ao(out, 'T', 'Float', 0, 'Temperature', unit='celsius')
    ao(out, 'P', 'Float', 0, 'Power', unit='percent')
    ao(out, 'S', 'Float', 0, 'Setpoint', unit='celsius')
    ao(out, 'maxHeatingRate', 'Float', 0, 'Max Heating Rate')
    ao(out, 'maxControlTemp', 'Float', 0, 'Max Control Temp')
    ao(out, 'minControlTemp', 'Float', 0, 'Min Control Temp')
    ao(out, 'maxElementTemp', 'Float', 0, 'Max Element Temp')
    ao(out, 'minElementTemp', 'Float', 0, 'Min Element Temp')
    return out

def instr_dict():
    """Returns a dictionary containing typical options for generic instrument object"""
    out = base_dict()
    ao(out, 'nSamples', 'Integer', 1, attr=['Hidden'])
    ao(out, 'camera', 'Role', ['camerapath', 'default'])
    ao(out, 'devices', 'List', attr=['Hidden'])
    ao(out, 'initTest', 'Progress', attr=['Hidden'])
    ao(out, 'closingTest', 'Progress', attr=['Hidden'])
    return out

def server_dict():
    out = base_dict()
    out['name']['current'] = 'server'
    ao(out, 'name', 'String', 'server')
    ao(out, 'isRunning', 'Boolean', False)
    ao(out, 'runningInstrument', 'String')
    ao(out, 'lastInstrument', 'String')
    ao(out, 'log', 'Log')
    return out


def smp_tree():
    """Tree for generical sample"""
    return  {'self': smp_dict()}

def instr_tree(): 
    """Tree for generical instrument"""
    return {'self': instr_dict(),
                'measure': {'self': measure_dict()}}

def tree_dict(): 
    """Main tree"""
    return {'self': server_dict(),
            'kiln': {'self': kiln_dict()}}


def create_tree(outFile, tree, path='/'):
    """Recursive tree structure creation"""
    for key, foo in tree.list():
        if outFile.has_node(path, key):
            logging.debug('Path already found:', path, key)
            continue
        logging.debug('%s %s %s', 'Creating group:', path, key)
        outFile.create_group(path, key, key)
        dest = path + key + '/'
        if outFile.has_node(dest):
            continue
        create_tree(outFile, tree.child(key), dest)
        
class Converter(object):
    name = 'Base Converter'
    file_pattern = '*'
    
    def __init__(self):
        self.outpath = ''
        self.interrupt = False
        self.progress = 0
        self.outFile = False
    
    def cancel(self):
        """Interrupt conversion and remove output file"""
        if self.outFile:
            self.outFile.close()
        if os.path.exists(self.outpath):
            os.remove(self.outpath)
     
    def convert(self):
        """Override this to do the real conversion"""
        assert False,'Unimplemented'
        
def search_registry(filename):
    """Find a matching converter for filename"""
    for converter in registry:
        if fnmatch(filename, converter.file_pattern):
            print 'Found converter', filename, converter.file_pattern
            return converter
    print 'No converter found', filename
    return False

def convert_file(path):
    """Do the conversion"""
    #TODO: implement threading and progress notification
    converter_class = search_registry(path)
    converter = converter_class()
    outpath = converter.convert(path)
    return outpath