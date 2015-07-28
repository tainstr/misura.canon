# -*- coding: utf-8 -*-
"""Option persistence."""
from traceback import print_exc

import ast
import option
from option import Option, sorter, tosave, typed_types
import store

from time import sleep
###
#  SQL PERSISTENCE
###

import collections


def define_columns(ctype, cols, fields=[]):
    """Define multiple columns `cols` as type `ctype`.`
    """
    pre = "' {},'".format(ctype)
    return "'" + pre.join(cols) + "' " + ctype

tabdef = define_columns('text', option.vkeys)

sys_keys = ('preset', 'fullpath', 'devpath')
# Set of all standard columns
opt_col_set = set(
    sys_keys + option.str_keys + option.int_keys + option.repr_keys)
# Definition strings by type
opt_col_def = define_columns('text', sys_keys)
opt_col_def += ',' + define_columns('text', option.str_keys)
opt_col_def += ',' + define_columns('integer', option.int_keys)
opt_col_def += ',' + define_columns('text', option.repr_keys)

# Column name: sql type, from sql function, to sql function
base_col_def = collections.OrderedDict()
cvt_str = ('text', unicode, unicode)
cvt_int = ('integer', int, int)
cvt_float = ('real', float, float)
cvt_repr = ('text', ast.literal_eval, repr)
cvt_bin = ('blob', False, False)
cvt_pickle = cvt_repr[:]
# cvt_pickle=('blob',lambda s: loads(str(s)),dumps)
cvt_bool = ('integer', bool, int)

converters = {'text': cvt_str,
              'real': cvt_float,
              'blob': cvt_bin,
              # Misura defined
              'integer': cvt_int,
              'float': cvt_float,
              'binary': cvt_bin,
              'string': cvt_str,
              'pickle': cvt_pickle,
              # Specific types
              'Meta': cvt_float,
              'Point': cvt_float,
              'Role': cvt_str,
              'RoleIO': cvt_str,
              'Rect': cvt_float,
              'Boolean': cvt_bool
              }

from_sql = {'integer': cvt_int,
            'real': cvt_float,
            'text': cvt_str,
            'blob': cvt_pickle}
for c in sys_keys + option.str_keys:
    base_col_def[c] = cvt_str
for c in option.int_keys:
    base_col_def[c] = cvt_int
for c in option.repr_keys:
    base_col_def[c] = cvt_repr

base_col_set = set(base_col_def.keys())


def define_dict(d):
    """Create column definition from dict"""
    r = ''
    for k, c in d.iteritems():
        r += "'{}' {}, ".format(k, c[0])
    return r[:-2]


def get_converter(mtype, sqltype):
    cvt = False
    # Typed converter
    if converters.has_key(mtype):
        # Type has preference
        cvt = converters[str(mtype)]
    else:
        # Then by type groups
        cvt = converters[str(sqltype)]
    return cvt


sep_field = '/'  # Field separator
sep_type = ':'  # Type separator


def get_typed_cols(opt):
    """Returns typed columns converter, typed columns names, extra columns names."""
    t = opt['type']
    # Typed columns definitions
    tk = list(option.type_keys[:])
    bt = option.bytype.get(t, False)
    if not bt:
        print 'Undefined sql type', opt
        return False
    cvt = get_converter(t, bt)
    if not cvt:
        print 'Undefined converter', t, bt
        return False

    # Multiply typed columns
    fields = []
    if bt == 'multicol':
        # Special definitions for multicol data
        if t == 'Role':
            fields = ('0', '1', '2')
        elif t == 'Point':
            fields = ('0', '1')
        elif t == 'Rect':
            fields = ('0', '1', '2', '3')
        elif t == 'Meta':
            fields = ('value', 'time', 'temp')
        elif t == 'RoleIO':
            tk += ['options{}0'.format(sep_field),
                   'options{}1'.format(sep_field),
                   'options{}2'.format(sep_field)]

    # Detect extra fields and add to definition
    extra = list(set(opt.keys()) - set(tk) - base_col_set)
    if len(extra):
        print 'found extras', tk, extra, opt

    # Remove ranges for non-numerics
    if bt in ('string', 'binary', 'multicol', 'pickle') or t in ('Meta', 'Log', 'Role', 'RoleIO', 'Boolean'):
        tk.remove('min')
        tk.remove('max')
        tk.remove('step')
    # No fields: append mtype to all tk
    if not len(fields) and t != 'RoleIO':
        tk1 = []
        for f in tk:
            # e.g.: "current:Integer", "current:Float", etc
            f1 = '{}{}{}'.format(f, sep_type, t)
            tk1.append(f1)
        tk = tk1[:]

    # Expand multi-field typed columns (dicts, etc)
    cols = []
    for field in fields:
        for c0 in tk:
            c1 = c0 + '/' + field
            cols.append(c1)
    if not len(fields):
        cols = tk[:]

    return cvt, cols, extra


def get_insert_cmd(entry, col_def):
    """Build sql statement to insert option `entry` in table defined by `col_def`"""
    t = entry['type']
    colnames = ''
    vals = []
    tc = get_typed_cols(entry)
    if not tc:
        return False
    cvt0, cols, extra = tc
    # Respect order
    for k, cvt in col_def.iteritems():
        # Detect composite columns
        mk = k.split(sep_field)
        if len(mk) == 2 and k in cols:
            mk, sub = mk
            if mk not in entry:
                print 'key not found', mk, entry
                continue
            # Meta are the only dict options
            # TODO: convert all Meta to lists?
            if t != 'Meta':
                sub = int(sub)
            try:
                v = entry[mk][sub]
            except:
                print 'Missing sub entry', k, mk, repr(sub), entry[mk], entry
                continue
        else:
            if k not in entry:
                continue
            if k in option.type_keys and k not in cols:
                continue
            v = entry[k]
        if v in ('None', None):
            continue
        v = cvt[2](v)
        colnames += "'{}', ".format(k)
        vals.append(v)
    # append fields
    if not len(vals):
        return False
    q = ('?,' * len(vals))[:-1]
    cmd = '({}) values ({});'.format(colnames[:-2], q)
    return cmd, vals


def parse_row(row, col_def=base_col_def):
    """Read single `row` using col_def table definition.
    Returns entry dictionary."""
    r = {}
    fields = set()
    for i, (k, cvt) in enumerate(col_def.iteritems()):
        v = row[i]
        # Skip empty columns
        if v is None:
            continue
# 		print 'converting',k,cvt[1],repr(v)
        v = cvt[1](v)
        if sep_field in k:
            mk, sub = k.split(sep_field)
            try:
                sub = int(sub)
            except:
                pass
            if not mk in r:
                # Start with an ordered dict
                r[mk] = collections.OrderedDict()
                fields.add(mk)
            r[mk][sub] = v
        elif sep_type in k:
            k, mt = k.split(sep_type)
        else:
            r[k] = v
    # Convert to lists all non-Meta types
    if str(r['type']) != 'Meta':
        for f in fields:
            # 			print 'found fields',f,fields
            # This will keep correct order
            r[f] = r[f].values()
    else:
        pass
# 		print 'Found Meta',fields,r
    return r


def tree_add_entry(tree, entry):
    """Insert option `entry` into tree `desc`"""
    fp = entry['fullpath'].split('/')
    fp.pop(-1)
    fp.pop(0)
    target = tree
    for dev in fp:
        if not dev in target:
            target[dev] = {}
        target = target[dev]
    e = entry.copy()
    for k in sys_keys:
        del e[k]
    if not 'self' in target:
        target['self'] = {}
    target['self'][e['handle']] = e
    return tree


def tree_from_rows(rows, col_def=base_col_def, tree=False):
    """Load all options from `rows` retrieved from `col_def` table definition dictionary
    into an option tree structure `tree`."""
    if not tree:
        tree = {}
    for row in rows:
        entry = parse_row(row, col_def)
        option.validate(entry)
        tree_add_entry(tree, entry)
    return tree


class SqlStore(store.Store):

    """Basic store using an sql table"""
    tabname = 'opt'
    cursor = None
    pragma = 0
    col_def = collections.OrderedDict()
    """Column definition"""

    def __init__(self, *a, **k):
        store.Store.__init__(self, *a, **k)

    def column_definition(self, opt):
        """Build sqlite table creation/alter statement for option `opt`"""
        # Typed columns definitions
        t = opt['type']
        tabname = self.tabname
        tc = get_typed_cols(opt)
        if not tc:
            return False
        cvt, cols, extra = tc

        r = []  # sql statements
        # If no table definition exists, create a basic one
        if not len(self.col_def):
            self.col_def = base_col_def.copy()
            # New table creation
            d = define_dict(self.col_def)
            r.append("create table '{}' ({});".format(tabname, d))

        # Update table definition as needed
        new = set(cols) - set(self.col_def.keys())
        if len(new):
            for k in cols:  # preserve order
                if not k in new:
                    continue
                r.append(
                    "alter table {} add column '{}' {}; \n".format(tabname, k, cvt[0]))

        # Add typed fields
        for c in cols:
            self.col_def[c] = cvt

        # Add extra fields
        for e in extra:
            self.col_def[e] = cvt_repr

        # Return mtype and sql statements
        return t, r

    def parse_table(self):
        """Parse opt table into ordered definition dictionary self.col_def"""
        self.cursor.execute('pragma table_info({});'.format(self.tabname))
        self.pragma = self.cursor.fetchall()
        print 'pragma', self.pragma
        for tup in self.pragma:
            col = tup[1]
            sqlt = tup[2]
            col_cvt = self.col_def.get(col, False)
            # Means this is an extra column (use cvt_repr)
            if not col_cvt:
                if sep_type in col:
                    n, mt = col.split(sep_type)
                    print 'parse table', col, n, mt
                    col_cvt = get_converter(mt, sqlt)
                else:
                    col_cvt = cvt_repr
            # remember column name and type
            self.col_def[col] = col_cvt
        return len(self.pragma)

    def clear_entry(self, entry, preset='default'):
        cmd = "delete from {} where fullpath='{}' and preset='{}';".format(
            self.tabname, entry['fullpath'], preset)
# 		print 'deleting:\n\t',cmd
        self.cursor.execute(cmd)
        return self.cursor.fetchall()

    def write_desc(self, desc=False, preset='default', parse=True):
        """Write typed tables"""
        if desc:
            self.desc = desc
        if parse:
            self.parse_table()
        clear = True
        for key, entry in self.desc.iteritems():
            # Add system keys
            entry['preset'] = preset
            kid = entry.get('kid', False)
            if kid:
                kid = kid.split('/')
                kid.pop(-1)  # handle
                if len(kid) < 2:
                    print 'invalid kid', kid, key, entry
                    continue
                entry['devpath'] = kid[-1]
                entry['fullpath'] = '/'.join(kid) + '/'
                if clear and len(self.pragma):
                    # Drop pre-existing data if first entry written
                    # 					print 'Clearing',self.clear_entry(entry,preset)
                    clear = False
            # Define columns
            cd = self.column_definition(entry)
            if not cd:
                print 'Invalid entry', key, entry
                continue
            t, sql = cd
            # Execute creation/altering
            for s in sql:
                # 				print 'write_desc altering:\n\t ',s
                self.cursor.execute(s)

            cmd, vals = get_insert_cmd(entry, self.col_def)
            cmd = "insert into '{}' {}".format(self.tabname, cmd)

# 			print 'write_desc inserting:\n\t',cmd,vals
            self.cursor.execute(cmd, vals)
        return True

    def read_tree(self, fullpath=False, preset='default'):
        """Read option for `fullpath` object in `preset`.
        If `fullpath` is omitted or False, all paths will be red."""
        self.parse_table()
        wh = ''
        if fullpath:
            wh += "fullpath='{}' ".format(fullpath)
        if preset:
            if len(wh):
                wh += 'and '
            wh += "preset='{}' ".format(preset)
        if len(wh) > 0:
            wh = " where " + wh
        cmd = "select * from {} ".format(self.tabname) + wh + ';'
        self.cursor.execute(cmd)
        rows = self.cursor.fetchall()
        tree = tree_from_rows(rows, self.col_def)
        # Directly return single-object dict
        if len(tree) == 1 and fullpath:
            return tree.values()[0]['self']
        # Return full tree
        return tree

    def write_tree(self, tree, preset='default', parse=True):
        """Write `tree` of options"""
        for k, v in tree.iteritems():
            if k == 'self':
                try:
                    self.write_desc(v, preset, parse=parse)
                except:
                    print_exc()
                continue
                parse = False
            # Auto-iterate
            self.write_tree(v, preset, parse)
            parse = False
    #####
    # LEGACY - only used by clientconf

    def read_table(self, cursor, tabname):
        cursor.execute("SELECT * from " + tabname)
        r = cursor.fetchall()
        self.desc = {}
        for row in r:
            entry = from_row(row)
            entry = self.update(entry)
            if not entry:
                continue
            self.desc[entry['handle']] = entry
        return self.desc

    def write_table(self, cursor, tabname, desc=False):
        """Dump configuration to a table having entry keys as columns"""
        # Create the table
        if not desc:
            desc = self.desc
        cursor.execute("drop table if exists " + tabname + ';')
        cmd = "create table " + tabname + " (" + tabdef + ");"
        cursor.execute(cmd)

        # Prepare the insertion command
        icmd = '?,' * len(option.vkeys)
        icmd = 'INSERT INTO ' + tabname + ' VALUES (' + icmd[:-1] + ')'

        # Reorder the options by priority
        values = desc.items()
        values.sort(sorter)
        prio = 0

        # Write the options
        for key, entry in values:
            prio += 1
            entry['priority'] = prio
            line = to_row(entry)
            if not line:
                print 'skipping line', key
                continue
            cursor.execute(icmd, line)
        return True

#####################


def from_row(row):
    """Create an Option object starting from a database row"""
    e = {}
    for i, k in enumerate(option.vkeys):
        if row[i] == '':
            continue
        e[k] = ast.literal_eval(row[i])
    return Option(**e)


def to_row(entry):
    """Encode the option into a database row"""
    if not tosave(entry):
        return False
    r = []
    for k in option.vkeys:
        if entry.has_key(k):
            r.append(repr(entry[k]))
        else:
            r.append('')
    return r
