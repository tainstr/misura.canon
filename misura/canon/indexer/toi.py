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

def current_Meta(opt):
    ret = []
    cur = opt['current']
    for k in ('temp', 'time', 'value'):
        v = cur[k]
        try:
            v = float(v)
        except:
            v = -1
        ret.append(v)
    return ret
    



#TODO: the combination of first 3 columns must be unique. Enforceable via sqlite? Or should I define a new combined key?
opt_base = [('uid', 'text'), ('version', 'text'),('fullpath', 'text'), ('handle', 'text')]

opt_unique = "uid, version, fullpath, handle"

# table name:  ([(name, type),], insert_func, unique)
toi_tables = {'option_Float': (opt_base[:]+[('current', 'real')], 
                               lambda a: [float(a['current'])], 
                               opt_unique),
              'option_Integer': (opt_base[:]+[('current', 'integer')], 
                                 lambda a: [int(a['current'])], 
                                 opt_unique),
              'option_Boolean': (opt_base[:]+[('current', 'bool')], 
                                 lambda a: [bool(a['current'])],
                                 opt_unique),
              'option_String': (opt_base[:]+[('current', 'text')], 
                                lambda a: [unicode(a['current'])], 
                                opt_unique),
              'option_Meta': (opt_base[:]+[('current', 'real'), ('time', 'real'), ('value','real')], 
                              current_Meta, 
                              opt_unique),
              
              'versions':([('uid', 'text'), ('version', 'text'), ('name', 'text'), ('date', 'text'), ('active', 'bool')], 
                          None, "uid, version"),
              'plots':([('hash', 'text'), ('uid', 'text'), ('version', 'text'), ('node', 'text'), ('name', 'text'), 
                        ('date', 'text'), ('script','text'), ('render', 'blob'), ('format', 'text')], 
                        None, "hash, uid, version"),
    }


special_funcs = {'Role': lambda a: [a['current'][0]],
                 'RoleIO': lambda a: [a['current'][0]+a['current'][2]]}

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
        
        index = 'create index if not exists idx_{0}_uid on {0}(uid)'.format(tab_name)
        cursor.execute(index)
        
        if not tab_name.startswith('option_'):
            continue
        
        view = '''CREATE VIEW IF NOT EXISTS view_recent_{0} AS
SELECT test.uid, test.zerotime, test.name, opt.fullpath, opt.handle, opt.current
FROM '{0}' AS opt INNER JOIN test ON test.uid = opt.uid
ORDER BY test.zerotime DESC'''.format(tab_name)
        cursor.execute(view)
        
    return True

def drop_tables(cursor):
    for tab_name in toi_tables.keys():
        cursor.execute("drop table if exists '{}'".format(tab_name))
        cursor.execute("drop view if exists 'view_recent_{}'".format(tab_name))
    drop_views(cursor)
    return True
        
def clear_test_uid(cursor, uid):
    """Remove test UID entries from all tables"""
    for tab_name in toi_tables.keys():
        cmd = "delete from '{}' where uid='{}'".format(tab_name, uid)
        print(cmd)
        cursor.execute(cmd)
    return True

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
        currents = current_func(opt)
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

def reorder_date(date):
    d = date.split(', ')
    if len(d)!=2:
        return date
    h, d = d
    d = '-'.join(d.split('/')[::-1])
    return d+' '+h

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
        vals = [plot_hash, shfile.uid, version, plot_name, title, reorder_date(date), script, sqlite3.Binary(render), render_format]
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
    vals = [shfile.uid, path, name, reorder_date(date), active]
    q = ('?,' * len(vals))[:-1]
    cmd = "insert or replace into 'versions' ({}) values ({});".format(colnames, q)
    cursor.execute(cmd, vals)
    return True

def index_file(cursor, shfile):
    """Scans the original version and all versions
    using index_version"""
    vers = shfile.get_versions()
    shfile.uid = shfile.get_node_attr('/conf', 'uid')
    i = 0
    active_version = shfile.active_version()
    for verpath, (vername, verdate) in vers.items():
        logging.debug('index_file', verpath, vername, verdate)
        active = verpath == active_version or len(vers)==1
        r = index_version(cursor, shfile, verpath, vername, verdate, active=active)
        i += r
    return r

views = [
'''
CREATE VIEW IF NOT EXISTS view_plots AS
SELECT t.file, p.name, p.date, t.name AS testName, t.zerotime, p.hash, p.uid, p.version, p.node
FROM plots as p
JOIN test AS t ON p.uid = t.uid
''',

'''
CREATE VIEW IF NOT EXISTS view_versions AS
SELECT t.file, p.name, p.date, p.version, t.name AS testName, t.zerotime, p.active
FROM versions as p
JOIN test AS t ON p.uid = t.uid
''',
    
'''
CREATE VIEW IF NOT EXISTS view_sample AS
SELECT t.file AS file, 
        s.current AS sample,
        t.name AS name, 
        t.uid AS uid,
        t.zerotime AS zerotime, 
        t.instrument AS instrument,  
        t.elapsed AS elapsed, 
        t.nSamples AS nSamples,
        s.version AS version,
        s.fullpath AS fullpath
        
FROM test as t
INNER JOIN option_String AS s ON t.uid = s.uid
INNER JOIN versions AS v ON v.uid = s.uid AND v.active = 1 AND v.version = s.version
WHERE s.handle = 'name' AND s.fullpath LIKE "/%/sample_/";
''',
]

view_names = ['view_plots', 'view_versions', 'view_sample']

for shape in ('Sintering', 'Softening', 'Sphere', 'HalfSphere', 'Melting'):
    view = '''CREATE VIEW IF NOT EXISTS view_sample_hsm_{0} AS
SELECT t.uid AS uid,
       s.fullpath AS fullpath,
       s.current AS current
FROM view_sample as t
INNER JOIN option_Meta AS s ON t.uid = s.uid AND t.version = s.version
WHERE s.handle = '{0}' AND s.fullpath = t.fullpath;
'''.format(shape) 
    views.append(view)
    view_names.append('view_sample_hsm_{0}'.format(shape))
    
    
    
views.append('''CREATE VIEW IF NOT EXISTS view_sample_hsm AS
SELECT t.file AS file,  
        s.sample AS sample,
        t.name AS name, 
        t.uid AS uid,
        t.zerotime AS zerotime, 
        t.instrument AS instrument,  
        t.elapsed AS elapsed, 
        t.nSamples AS nSamples,
        s.version AS version,
        s.fullpath AS fullpath,
        sint.current AS sintering,
        soft.current AS softening,
        sph.current AS sphere,
        hsph.current AS halfSphere,
        melt.current AS melting
FROM test as t
INNER JOIN view_sample AS s ON t.uid = s.uid
INNER JOIN view_sample_hsm_Sintering AS sint ON s.fullpath = sint.fullpath AND s.uid = sint.uid
INNER JOIN view_sample_hsm_Softening AS soft ON s.fullpath = soft.fullpath AND s.uid = soft.uid
INNER JOIN view_sample_hsm_Sphere AS sph ON s.fullpath = sph.fullpath AND s.uid = sph.uid
INNER JOIN view_sample_hsm_HalfSphere AS hsph ON s.fullpath = hsph.fullpath AND s.uid = hsph.uid
INNER JOIN view_sample_hsm_Melting AS melt ON s.fullpath = melt.fullpath AND t.uid = melt.uid
''')

view_names.append('view_sample_hsm')


def create_views(cur):
    for view in views:
        cur.execute(view)
    return True


def drop_views(cursor):
    for tab_name in view_names:
        cursor.execute("drop view if exists '{}'".format(tab_name))
    return True
