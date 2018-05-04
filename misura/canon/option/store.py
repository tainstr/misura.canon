# -*- coding: utf-8 -*-
"""Option persistence."""
from traceback import format_exc, print_exc
import ast
import os

from .. import logger
unicode_func = logger.unicode_func
from .option import Option, sorter, tosave

from functools import cmp_to_key

class Store(object):

    """Used for iterative Option loading from file, db, etc"""

    def __init__(self, kid=''):
        self.kid = kid
        self.desc = {}
        self.log = logger.Log
        self.priority = 0
        self.priorities = []

    def update(self, opt):
        """Update base KID and priority"""
        # add priority field if not present
        # TODO: remove all priority management and substitute with
        # collections.OrderedDict
        if 'priority' in opt:
            self.priority = int(opt['priority'])
        else:
            self.priority += 1
            while self.priority in self.priorities:
                self.priority += 1
            opt['priority'] = self.priority
        self.priorities.append(self.priority)
        opt.set_base_kid(self.kid)
        return opt

    def validate(self):
        failed = {}
        if len(self.desc) == 0:
            return failed
        for key, entry in self.desc.items():
            entry = self.update(entry)
            if entry:
                self.desc[key] = entry
            else:
                failed[key] = entry
                print('Validation failed:', key, entry, ' -- REMOVED')
        return failed

    @classmethod
    def read(cls, obj):
        s = cls(obj)
        return s.desc


assign_sym = '=>'
concat_sym = '|;|'
assign_sym_mask='@!@assign!_!sym!_!mask@!@'
concat_sym_mask = '@!@concat!_!sym!_!mask@!@'

def from_string(line):
    """Create an Option object starting from a csv text line"""
    line = unicode_func(line, encoding='utf-8', errors='replace')
    if line[0] == '#' or len(line) < 5 or line.startswith('import:'):
        return False
    ents = line.split(concat_sym)
    entry = {}
    for ent in ents:
        ent = ent.replace(assign_sym_mask, 
                          assign_sym).replace(concat_sym_mask, 
                                              concat_sym)
        key, val = ent.split(assign_sym)
        val = ast.literal_eval(val)
        entry[key] = val
    return Option(**entry)


def to_string(opt):
    """Encodes the option into a string"""
    entry = opt.entry
    if not tosave(entry):
        return False
    line = ''
    for key, val in entry.items():
        if key in ['kid']:
            continue
        val = '{!r}'.format(val)
        val = val.replace(assign_sym, 
                          assign_sym_mask).replace(concat_sym, 
                                                        concat_sym_mask)
        
        line += '{!s}{!s}{!s}{!s}'.format(key, assign_sym, val, concat_sym)
    line = line[0:-3]
    return line


###
# CSV FILE PERSISTENCE
###

class CsvStore(Store):

    def __init__(self, filename=False, kid=''):
        Store.__init__(self, kid=kid)
        self.filename = filename
        if filename:
            self.read_file(filename)

    def merge_file(self, filename, enable_imports=True):
        out = open(filename, 'r')
        lin = -1
        line = ''
        for raw in out:
            lin += 1

            if raw.startswith('#EOF'):
                break
            if raw.endswith('\\\n'):
                raw = raw.strip('\\\n')
                line += raw + '\n'
                continue

            line += raw

            if line.lower().startswith('import:'):
                if not enable_imports:
                    line = ''
                    continue
                line = line.strip('\n')
                # Removing leading whites
                line = line[7:]
                line = line.lstrip()
                if not line.endswith('.csv'):
                    line += '.csv'
                # Relative imports
                if line.startswith('.') or not line.startswith('/'):
                    path, name = os.path.split(filename)
                    line = os.path.join(path, line)
                self.merge_file(line)
                line = ''
                continue
            try:
                entry = from_string(line)
            except:
                self.log.error(
                    'Reading config:', filename, '\nat entry: ', lin, line, '\n', format_exc())
                line = ''
                continue
            if not entry:
                line = ''
                continue
            entry = self.update(entry)
            line = ''
            if not entry:
                continue
            self.desc[entry['handle']] = entry
        return self.desc

    def read_file(self, filename=False):
        """Start a new import of a configuration file"""
        if not filename:
            filename = self.filename
        if not os.path.exists(filename):
            self.log.error('Read: non existent path', filename)
            return False
        self.filename = filename
        self.desc = {}
        self.merge_file(filename)
        return self.desc

    def write_file(self, filename=False):
        if not filename:
            filename = self.filename
        out = open(filename, 'w')
        values = list(self.desc.items())
        values.sort(key=cmp_to_key(sorter))
        prio = 0
        for key, entry in values:
            prio += 1
            entry['priority'] = prio
            line = False
            try:
                line = to_string(entry)
            except:
                print('to_string error on:', key, entry)
                print_exc()
            if not line:
                continue
            out.write(line + '\n')
        out.close()


class ListStore(Store):

    """Simple store used to read a list of dicts representing single entries."""

    def __init__(self, lst=[]):
        Store.__init__(self)
        self.read_list(lst)

    def read_list(self, lst):
        for entry in lst:
            ks = list(entry.keys())
            # intercept simple update requests: opt is key:val
            if len(ks) == 1:
                k = ks[0]
                if k not in self.desc:
                    print('Missing update key', entry)
                    continue
                self.desc[k]['current'] = entry[k]
                continue
            
            # Else, add the option
            entry = self.update(Option(**entry))
            if not entry:
                continue
            self.desc[entry['handle']] = entry
        return self.desc
