# -*- coding: utf-8 -*-
"""Total Option Indexing"""

import hashlib
import os
from time import time
try:
    import cPickle as pickle
except:
    import pickle
    unicode = str
from traceback import format_exc
import sqlite3
import functools
import datetime

from misura.canon.csutil import unlockme, lockme, enc_options, sharedProcessResources

import tables
from tables.nodes import filenode

from .. import csutil, option
from ..logger import get_module_logging
logging = get_module_logging(__name__)

from misura.canon.indexer.interface import SharedFile

def current_Meta(current):
    return [float(current['temp']),
            float(current['time']),
            float(current['value'])]
    
def current_RoleIO(current):
    return [current[0]+current[2]]


#TODO: the combination of first 3 columns must be unique. Enforceable via sqlite? Or should I define a new combined key?
opt_base = [('uid', 'text'), ('version', 'text'),('fullpath', 'text'), ('handle', 'text')]

opt_unique = "uid, version, fullpath, handle"

# table name:  ([(name, type),], insert_func, unique)
toi_tables = {'option_Float': (opt_base[:]+[('current', 'real')], 
                               lambda a: [float(a)], 
                               opt_unique),
              'option_Integer': (opt_base[:]+[('current', 'integer')], 
                                 lambda a: [int(a)], 
                                 opt_unique),
              'option_Boolean': (opt_base[:]+[('current', 'bool')], 
                                 lambda a: [bool(a)],
                                 opt_unique),
              'option_String': (opt_base[:]+[('current', 'text')], 
                                lambda a: [unicode(a)], 
                                opt_unique),
              'option_Meta': (opt_base[:]+[('current', 'real'), ('time', 'real'), ('value','real')], 
                              current_Meta, 
                              opt_unique),
              
              'versions':([('uid', 'text'), ('version', 'text'), ('title', 'text'), ('date', 'text'), ('active', 'bool')], 
                          None, "uid, version"),
              'plots':([('hash', 'text'), ('uid', 'text'), ('version', 'text'), ('name', 'text'), ('title', 'text'), 
                        ('date', 'text'), ('script','text'), ('render', 'blob'), ('format', 'text')], 
                        None, "hash, uid, version"),
    }


special_funcs = {'Role': lambda a: [a[0]],
                 'RoleIO': lambda a: [a[0]+a[2]]}

aliases = {'Number': 'Float', 'Progress': 'Float', 'Time': 'Float'}

for txt in ('TextArea', 'Script', 'Section', 'FileList', 'Date', 'Preset', 'Button', 'ThermalCycle', 'Role', 'RoleIO'):
    aliases[txt] ='String'

def get_table_definition(listdef, unique_def=False):
    r = [e[0]+ ' ' + e[1] for e in listdef]
    r = '('+', '.join(r)
    if unique_def:
        r += ', constraint unique_def unique ({})'.format(unique_def)
    r +=' )'
    return r
    
def get_column_names(listdef):
    cols = ["'"+e[0]+"'" for e in listdef]
    ret = ', '.join(cols)
    return ret

def create_tables(cursor):
    """Creates all option tables"""
    for tab_name, (tab_def, cur_func, unique_def) in toi_tables.items():
        tab_def = get_table_definition(tab_def, unique_def)
        cmd = 'create table if not exists {} {}'.format(tab_name, tab_def)
        cmd += ';'
        cursor.execute(cmd)
    return True
        
def purge_test_uid(cursor, uid):
    """Remove test UID entries from all tables"""
    pass

def index_option(cursor, uid, version, path, opt):
    """Insert an option into its table"""
    otype = aliases.get(opt['type'], opt['type'])
    tab_name = 'option_'+otype
    if tab_name not in toi_tables:
        #logging.debug('Unsupported option type', opt['type'], tab_name, path, opt['handle'])
        return False
    listdef, current_func, unique_def = toi_tables[tab_name]
    current_func = special_funcs.get(opt['type'], current_func)
    colnames = get_column_names(listdef)
    try:
        currents = current_func(opt['current'])
    except:
        logging.info('Could not normalize option value', path, opt['handle'], opt['current'], current_func)
        return False
    vals = [uid, version, path, opt['handle']]+currents
    q = ('?,' * len(vals))[:-1]
    assert len(listdef)==len(vals), 'Table definition differs from values provided {} {}'.format(listdef, vals)
    cmd = "insert or replace into '{}' ({}) values ({});".format(tab_name, colnames, q)
    cursor.execute(cmd, vals)
    return True

def index_desc(cursor, uid, version, desc):
    """Parse a full configuration dictionary"""
    fullpath = desc['fullpath']['current']
    i = 0
    for handle, opt in desc.items():
        if handle == 'self':
            continue
        i += index_option(cursor, uid, version, fullpath, opt)
    return i

def index_tree(cursor, uid, version, tree):
    """Index all current values of options contained in tree"""
    for k, sub in tree.items():
        if k == 'self':
            try:
                index_desc(cursor, uid, version, sub)
            except:
                logging.warning(format_exc())
            continue
        # Auto-iterate
        index_tree(cursor, uid, version, sub)

def index_plots(cursor, shfile, version=''):
    """Index all plots contained in a specific `version`"""
    if version=='':
        return False
    i = 0
    p = shfile.get_plots(version=version, render=True)
    if not p:
        return False
    for plot_name, (title, date, render, render_format) in p.items():
        logging.debug('indexing', version, plot_name, title, date)
        script_path =shfile._versioned('/plot/'+plot_name+'/script', version=version)
        script = shfile.file_node(script_path)
        plot_hash = hashlib.md5(script).hexdigest()
        vals = [plot_hash, shfile.uid, version, plot_name, title, date, script, sqlite3.Binary(render), render_format]
        colnames = get_column_names(toi_tables['plots'][0])
        q = ('?,' * len(vals))[:-1]
        cmd = "insert or replace into 'plots' ({}) values ({});".format(colnames, q)
        cursor.execute(cmd, vals)
        i += 1
    return i

def index_version(cursor, shfile, path='', name='Original', date='', active=False):
    """ Index a specific verson with index_tree
    populate versions and plots tables"""
    logging.debug('index_version', path, name, date)
    tree = shfile.conf_tree(path=shfile._versioned('/conf', version=path))
    index_tree(cursor, shfile.uid, path, tree)
    index_plots(cursor, shfile, path)
    # versions table entry
    colnames = get_column_names(toi_tables['versions'][0])
    vals = [shfile.uid, path, name, date, active]
    q = ('?,' * len(vals))[:-1]
    cmd = "insert or replace into 'versions' ({}) values ({});".format(colnames, q)
    cursor.execute(cmd, vals)
    return True

def index_file(cursor, shfile):
    """Scans the original version and all versions
    using index_version"""
    vers = shfile.get_versions()
    i = 0
    for verpath, (vername, verdate) in vers.items():
        logging.debug('index_file', verpath, vername, verdate)
        r = index_version(cursor, shfile, verpath, vername, verdate, verpath==shfile.version)
        i += r
    return r




