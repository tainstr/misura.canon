# -*- coding: utf-8 -*-
"""Option persistence."""

defined_attr = {'Binary': 'Binary blob',
                'Runtime': 'Do not save this value',
                'History': 'Chronological changes are recorded during acquisition',
                'Hardware': 'Value should be synced with external device upon configuration load',
                'Hot': 'Read from memory during acquisition',
                'ReadOnly': 'The client cannot change this value',
                'Hidden': 'Never visible to the user',

                'Enabled': 'Option enabled',
                'Disabled': 'Option disabled',

                # Script attrs:
                'ExeSummary': 'Execute at summary time',
                'ExeEnd': 'Execute at the end of the test',
                'ExeAlways': 'Execute at each supervisor iteration'
                }

defined_types = {
    # Binary types
    "Binary": 'Binary blob',
    'Image': 'Image',

    # Float types
    "Float": 'Float number',
    "Progress": 'Progress indicator',
                "Time": 'Time',

                # Integer types
                "Integer": 'Integer number',
                "Boolean": 'True/False',

                # String types
                "Script": 'Executable script',
                "Section": 'Section header',
                "FileList": 'List of file names',
                "String": 'Text field',
                "FilePath": 'File system path',
                "TextArea": 'Long text field',
                "Date": 'Date',
                "Preset": 'Persistently saved preset name',
                "ThermalCycle": 'Thermal cycle curve name',
                # None types (as empty strings)
                "Button": 'Getting this option triggers a server-side action',

                # Fixed multicolumn types
                "Meta": 'Metadata composite field (Time, Temperature, Value)',
                'Rect': 'Rectangle x,y,w,h',
                'Point': 'Point x,y',
                'Log': 'Log messages',
                "Role": 'Generic device role',
                "RoleIO": 'Generic input/output Role',

                # Variable multicolumn types
                "Table": 'A table of data',

                # Pickled types
                "ReadOnly": 'The user cannot change this',
                "Hidden": 'Never visible to the user',
                "Chooser": 'Predefined multiple choices',
                "List": 'List of objects',
                'Profile': 'Image profile',
}

typed_types = {'integer': ('Integer', 'Boolean'),
               'float': ('Float', 'Progress', 'Time'),
               'binary': ('Binary', 'Image'),
               'string': ('String', 'TextArea', 'Script', 'Section', 'FileList', 'Date', 'Preset', 'Button', 'ThermalCycle'),
               'multicol': ('Meta', 'Rect', 'Point', 'Role', 'RoleIO'),
               'pickle': ('ReadOnly', 'Hidden', 'Chooser', 'List', 'Profile', 'Table', 'Log'),
               }
bytype = {}
for k, v in typed_types.iteritems():
    for t in v:
        bytype[t] = k

num_types = typed_types['integer'] + typed_types['float']

vkeys = "handle,name,current,factory_default,attr,type,writeLevel,readLevel,mb,step,max,min,options,parent,values,flags,unit,csunit,kid,priority".split(
    ',')

str_keys = ('handle', 'name', 'type', 'parent', 'unit', 'csunit', 'kid')
int_keys = ('readLevel', 'writeLevel', 'mb', 'priority')
type_keys = ('current', 'factory_default', 'min', 'max', 'step')
repr_keys = ('attr', 'flags', 'options', 'values')  # and any other....

nowrite = set(['Binary', 'Runtime'])  # attributes which should not be saved
# TODO: limit the nowrite just to the current and factory_default properties.


def tosave(entry):
    """Determine if this option should be saved or not"""
    if len(nowrite.intersection(set([entry['type']]))) > 0:
        print 'nowrite entry by type', entry
        return False
    if entry.has_key('attr'):
        if len(nowrite.intersection(set(entry['attr']))) > 0:
            print 'nowrite entry by attr', entry
            return False
    return True


def sorter(a, b):
    """Option sorter"""
    if not a[1].has_key('priority') and not b[1].has_key('priority'):
        return 0
    elif a[1].has_key('priority') and not b[1].has_key('priority'):
        return -1
    elif b[1].has_key('priority') and not a[1].has_key('priority'):
        return +1
    elif a[1]['priority'] > b[1]['priority']:
        return +1
    elif a[1]['priority'] == b[1]['priority']:
        return 0
    else:
        return -1


def prop_sorter(a, b):
    if not a.has_key('priority') and not b.has_key('priority'):
        return 0
    elif a.has_key('priority') and not b.has_key('priority'):
        return -1
    elif b.has_key('priority') and not a.has_key('priority'):
        return +1
    elif a['priority'] > b['priority']:
        return +1
    elif a['priority'] == b['priority']:
        return 0
    else:
        return -1


def ao(d, handle=False, type='Empty', current=None, name=False,
       priority=-1, parent=False, flags={}, unit=None, options=False,
       values=False, attr=[], **kw):
    if not handle:
        return d
    if current is None:
        if bytype[type] in ('float', 'integer'):
            current = 0
        elif type == 'List':
            current = []
        # TODO: remove point, does no meaning anymore!
        elif type == 'Meta':
            current = {'temp': 'None', 'time': 'None', 'value': 'None'}
        elif type == 'Rect':
            current = [0, 0, 640, 480]
        elif type == 'Point':
            current = [0, 0]
        elif type == 'Log':
            current = [0, 'log']  # level, message
        elif type == 'Profile':
            current = []
        elif type == 'Role':
            current = ['None', 'default']
        else:
            current = ''
    if not name:
        name = handle
    if priority < 0:
        priority = len(d)

    ent = {'priority': priority, 'handle': handle, 'name': name, 'current': current,
           'factory_default': current, 'readLevel': 0, 'writeLevel': 0,
           'type': type, 'kid': 0, 'attr': attr, 'flags': flags, 'parent': parent}
    ent.update(kw)
    if values is not False:
        ent['values'] = values
    if options is not False:
        ent['options'] = options
    elif type == 'RoleIO':
        ent['options'] = ['None', 'default', 'None']
    elif type in ('Chooser', 'FileList'):
        ent['options'] = []
    if unit is not None:
        ent['unit'] = unit
    elif type == 'Meta':
        ent['unit'] = {'time': 'second', 'temp': 'celsius', 'value': 'None'}
    ent['kid'] = str(id(ent))
    ent = validate(ent)
    d[handle] = ent
    return d


def validate(entry):
    """Verify coherence of option `entry`"""
    key = entry.get('handle', False)
    if not key:
        print 'No handle for', entry, ': skipping!'
        return False
    # Type guessing
    etype = entry.get('type', False)
    if etype is False:
        cur = entry.get('current', None)
        if type(cur) == type(1):
            etype = 'Integer'
        elif type(cur) == type(''):
            etype = 'String'
        elif type(cur) == type(1.):
            etype = 'Float'
        elif type(cur) in [type([]), type((1,))]:
            etype = 'List'
        elif type(cur) == type({}):
            if set(cur.keys()) == set('temp', 'time', 'value'):
                etype = 'Meta'
        else:
            print 'No type for', entry, ': skipping!'
            return False
        entry['type'] = etype
    # redundancy integration
    if not entry.has_key('current'):
        if etype in num_types:
            v = 0
        elif etype == 'List':
            v = []
        elif etype == 'Meta':
            v = {'temp': 'None', 'time': 'None', 'value': 'None'}
        elif etype == 'Log':
            v = [0, 'log']
        elif etype == 'Profile':
            v = []
        elif etype in ['String', 'Binary', 'FilePath']:
            v = ''
        elif etype == 'Rect':
            v = [0, 0, 0, 0]
        elif etype == 'Point':
            v = [0, 0]
        elif etype == 'Role':
            v = ['None', 'default']
        else:
            v = ''
        entry['current'] = v

    if etype == 'RoleIO' and not entry.has_key('options'):
        entry['options'] = ['None', 'default', 'None']
    if not entry.has_key('flags'):
        entry['flags'] = {}
    # 0=always visible; 1=user ; 2=expert ; 3=advanced ; 4=technician ;
    # 5=developer; 6=never visible
    if not entry.has_key('readLevel'):
        entry['readLevel'] = 0
    if not entry.has_key('writeLevel'):
        # Inizializzo al livello readLevel+1
        entry['writeLevel'] = entry['readLevel'] + 1
    if entry.has_key('current') and not entry.has_key('factory_default'):
        entry['factory_default'] = entry['current']
    elif entry.has_key('factory_default') and not entry.has_key('current'):
        entry['current'] = entry['factory_default']
    if not entry.has_key('name'):
        entry['name'] = entry['handle'].replace('_', ' ').capitalize()
    if not entry.has_key('parent'):
        entry['parent'] = False
    if not entry.has_key('unit'):
        if entry['type'] == 'Meta':
            entry['unit'] = {
                'time': 'second', 'temp': 'celsius', 'value': 'None'}
        else:
            entry['unit'] = 'None'
        if entry['type'] == 'Meta':
            entry['unit']['temperature'] = 'celsius'
            entry['unit']['time'] = 'second'
    if entry['current'] == None:
        entry['current'] = 'None'
    # add attr field if not present
    if not entry.has_key('attr'):
        entry['attr'] = []
    # add maximum=1 for Progress
    if etype == 'Progress' and not entry.has_key('max'):
        entry['max'] = 1
    return entry


read_only_keys = ['handle', 'type', 'unit']


class Option(object):

    """An Option object"""
    _keys = ['handle', 'type', 'attr', 'name', 'current', 'factory_default',
             'current', 'options', 'values', 'unit', 'parent', 'flags', 'priority']
    """Mandatory keys"""
    _entry = {}
    _kid = ''
    _priority = 0

    def __init__(self, **kw):
        self.entry = kw

    def iteritems(self):
        return self.entry.iteritems()

    def itervalues(self):
        return self.entry.itervalues()

    def __str__(self):
        """String representation useful for printing purposes"""
        s = 'Option object: ' + self._entry['handle'] + '\n'
        for k, v in self.entry.iteritems():
            if k == 'handle':
                continue
            s += '\t |%s=%s\n' % (k, v)
        return s

    def __repr__(self):
        """Pythonic representation"""
        return 'Option(**%s)' % repr(self.entry)

    def pretty_format(self):
        r = '{'
        for key, val in self.entry.iteritems():
            # Avoid obvious keys
            if key in ['kid', 'priority', 'factory_default'] or \
                    (key == 'comment' and 'dummy' in val) or \
                    (key == 'readLevel' and val == 0) or \
                    (key == 'writeLevel' and val == self['readLevel'] + 1) or \
                    (key == 'parent' and val is False) or \
                    (key == 'unit' and val == 'None') or \
                    (key == 'flags' and val == {}) or \
                    (key == 'attr' and val == []):
                continue
            if type(val) == type(''):
                if '\n' in val:
                    val = '"""' + val + '"""'
                else:
                    val = repr(val)
            else:
                val = repr(val)
            r += '"%s": %s, \n\t' % (key, val)
        r += '}'
        return r

    def __eq__(self, other):
        """Checks the equality between two Option objects"""
        return self._entry == getattr(other, '_entry', None)

    def __delitem__(self, k):
        if k not in self._keys:
            print 'Requested key does not exist'
            return False
        if self._entry.has_key(k):
            del self._entry[k]
        return True

    def pop(self, k):
        return self._entry.pop(k)

    @property
    def entry(self):
        """Return a dictionary entry"""
        return self._entry

    @entry.setter
    def entry(self, e):
        """Sets the dictionary entry"""
        for k in self._keys:
            self._entry[k] = None
        en = validate(e)
        if en:
            self._entry = en

    def get(self, *arg):
        # If key does not exists and default was specified in kw, return
        # default
        arg = list(arg)
        if len(arg) == 0:
            arg.append('current')
        k = arg[0]
        # If keyword does not exist, return default if specified
        if len(arg) == 2 and not self._entry.has_key(k):
            return arg[1]
        # Return the value or raise exception
        return self._entry[k]
    __getitem__ = get

    def __contains__(self, key):
        return key in self._entry

    def set(self, *arg):
        # If just one argument, pass to `current` key
        if len(arg) == 1:
            k = 'current'
            v = arg[0]
        else:
            # traditional key,val was passed
            k, v = arg
        if k in read_only_keys:
            print 'Read only key!', k
            return
        self._entry[k] = v
    __setitem__ = set

    def has_key(self, k):
        return self._entry.has_key(k)

    def keys(self):
        return self._entry.keys()

    def set_base_kid(self, kid):
        self._entry['kid'] = kid + self._entry['handle']

    ###
    # Conversions
    ###

    def validate(self):
        self._entry = validate(self._entry)

    def copy(self):
        return Option(**self._entry)

    def migrate_from(self, old):
        """Migrate Option from `old`. 
        Notice: the first migration always happens between hard-coded `old` and saved configuration file in self."""
        # These keys can only change on software updates.
        # So, their `old` value cannot be overwritten and must be retained
        for k in ('name', 'factory_default', 'readLevel', 'writeLevel', 'mb', 'unit'):
            if old.has_key(k):
                self._entry[k] = old[k]
        # Retain special attributes
        oa = set([])
        na = set([])
        if self._entry.has_key('attr'):
            na = set(self._entry['attr'])
        if old.has_key('attr'):
            oa = set(old['attr'])
        # Update user-modifiable attributes
        for a in ('History', 'ExeSummary', 'ExeEnd', 'ExeAlways', 'Enabled', 'Disabled'):
            # Add if added in new
            if a in na:
                oa.add(a)
            # Remove if missing from new
            elif a in oa:
                oa.remove(a)
        # Keep everything else
        self._entry['attr'] = list(oa)
        ot = old['type']
        nt = self['type']
        # Reset table option if its definition changed
        if nt == 'Table':
            new_def = [h[1] for h in self['current'][0]]
            old_def = [h[1] for h in old['current'][0]]
            if new_def != old_def:
                print 'Incompatible table definition', self['handle'], new_def, old_def
                self['current']=[self['current'][0]]
        # No type change: exit
        if ot == nt:
            return
        # Hard-coded 'old' type differs from configured type:
        # Import all special keys that might be defined in old but missing in
        # self
        for k in ('type', 'step', 'max', 'min', 'options', 'values'):
            if old.has_key(k):
                self._entry[k] = old[k]
        if not self._entry.has_key('current'):
            return
        nc = self._entry['current']
        # New current value migration from new type (red) to old type (hard
        # coded)
        try:
            if ot in ('String', 'TextArea', 'FileList', 'Section'):
                nc = str(nc)
            elif ot == 'Integer':
                nc = int(nc)
            elif ot in ('Float'):
                nc = float(nc)
            elif ot == 'Button':
                nc = ''
            self['current'] = nc
        except:
            print 'Impossible to migrate current value', old['handle'], nc, ot
            # Remove current key
            del self._entry['current']
