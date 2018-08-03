# -*- coding: utf-8 -*-
"""Option persistence."""
from ..logger import get_module_logging
logging = get_module_logging(__name__)

defined_attr = {'Binary': 'Binary blob',
                'Runtime': 'Do not save this value',
                'History': 'Chronological changes are recorded during acquisition',
                'Event': 'Recorded changes are to be considered independent events (not sampling of a continuous change)',
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
for k, v in typed_types.items():
    for t in v:
        bytype[t] = k

num_types = typed_types['integer'] + typed_types['float']

vkeys = "handle,name,current,factory_default,attr,type,writeLevel,readLevel,mb,step,max,min,options,parent,values,flags,unit,csunit,kid,priority".split(
    ',')

str_keys = ('handle', 'name', 'type', 'parent', 'unit', 'csunit', 'kid','aggregate')
int_keys = ('readLevel', 'writeLevel', 'mb', 'priority')
type_keys = ('current', 'factory_default', 'min', 'max', 'step')
repr_keys = ('attr', 'flags', 'options', 'values')  # and any other....

nowrite = set(['Binary', 'Runtime'])  # attributes/types which should not be saved
# TODO: limit the nowrite just to the current and factory_default properties.


def tosave(entry, excl=[]):
    excl = set(excl).union(nowrite)
    """Determine if this option should be saved or not"""
    if len(excl.intersection(set([entry['type']]))) > 0:
        logging.debug('nowrite entry by type', entry)
        return False
    if 'attr' in entry:
        if len(excl.intersection(set(entry['attr']))) > 0:
            logging.debug('nowrite entry by attr', entry)
            return False
    return True

def sorter(a, b):
    """Option sorter"""
    if 'priority' not in a[1] and 'priority' not in b[1]:
        return 0
    elif 'priority' in a[1] and 'priority' not in b[1]:
        return -1
    elif 'priority' in b[1] and 'priority' not in a[1]:
        return +1
    elif a[1]['priority'] > b[1]['priority']:
        return +1
    elif a[1]['priority'] == b[1]['priority']:
        return 0
    else:
        return -1


def prop_sorter(a, b):
    if not a and not b:
        return 0
    if not a:
        return +1
    if not b:
        return -1
    
    if (not 'priority' in a) and (not 'priority'  in b):
        return 0
    elif 'priority' in a and not ('priority' in b):
        return -1
    elif 'priority' in b and not ('priority' in a):
        return +1
    elif a['priority'] > b['priority']:
        return +1
    elif a['priority'] == b['priority']:
        return 0
    else:
        return -1


def ao(d, handle=False, type='Empty', current=None, name=False,
       priority=-1, parent=False, flags=False, unit=None, options=False,
       values=False, attr=[], **kw):
    if not handle:
        logging.debug('ao: No handle!', handle)
        return d
    flags = flags or {}
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
    if priority < 0 or priority==None:
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
        logging.debug('No handle for', entry, ': skipping!')
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
        elif isinstance(cur, {}):
            if set(cur.keys()) == set('temp', 'time', 'value'):
                etype = 'Meta'
        else:
            logging.debug('No type for', entry, ': skipping!')
            return False
        entry['type'] = etype
    # redundancy integration
    if 'current' not in entry:
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

    if etype == 'RoleIO' and 'options' not in entry:
        entry['options'] = ['None', 'default', 'None']
    if 'flags' not in entry:
        entry['flags'] = {}
    # 0=always visible; 1=user ; 2=expert ; 3=advanced ; 4=technician ;
    # 5=developer; 6=never visible
    if 'readLevel' not in entry:
        entry['readLevel'] = 0
    if 'writeLevel' not in entry:
        # Inizializzo al livello readLevel+1
        entry['writeLevel'] = entry['readLevel'] + 1
    if 'current' in entry and 'factory_default' not in entry:
        entry['factory_default'] = entry['current']
    elif 'factory_default' in entry and 'current' not in entry:
        entry['current'] = entry['factory_default']
    if 'name' not in entry:
        entry['name'] = entry['handle'].replace('_', ' ').capitalize()
    if 'parent' not in entry:
        entry['parent'] = False
    if entry['parent'] == entry['handle']:
        logging.critical('Option parent must differ from handle!')
        entry['parent'] = False
    if 'unit' not in entry:
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
    if 'attr' not in entry:
        entry['attr'] = []
    # add maximum=1 for Progress
    if etype == 'Progress' and 'max' not in entry:
        entry['max'] = 1
    return entry


read_only_keys = ['handle', 'type']


def namingConvention(path, splt='/'):
    """If path pertains to a sample property, returns its sample number and option name"""
    if not splt + 'sample' in path:
        return path, None
    if path.endswith(splt):
        path = path[:-1]
    v = path.split(splt)
    # Find sample number
    for i, d in enumerate(v):
        if not d.startswith('sample'):
            continue
        idx = int(d[6:])
        break
    # Find option name
    # If it is properly a sample option
    if v[-2].startswith('sample'):
        return v[-1], idx
    # Otherwise, return path starting from sample
    return splt.join(v[i:]), idx


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
        for item in self.entry.items():
            yield item
            
    def items(self):
        return self.entry.items()
    
    def keys(self):
        return self.entry.keys()

    def itervalues(self):
        for value in self.entry.values():
            yield value
    
    def values(self):
        return self.entry.values()
    
    def __len__(self):
        return len(self.entry)

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
        for key, val in self.entry.items():
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
            logging.debug('Requested key does not exist')
            return False
        if k in self._entry:
            del self._entry[k]
        return True

    def pop(self, *a):
        return self._entry.pop(*a)

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
        if len(arg) == 2 and k not in self._entry:
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
            logging.debug('Read only key!', k, v, self._entry['handle'])
            return
        self._entry[k] = v
    __setitem__ = set

    def has_key(self, k):
        return k in self._entry


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
        for k in ('name', 'factory_default', 'readLevel', 'writeLevel', 
                  'mb', 'unit', 'csunit', 'parent', 'error'):
            if k in old:
                self._entry[k] = old[k]
                
                
        upkeys = ['step', 'max', 'min', 'options', 'values', 'visible', 'precision',
                  'unit']
        
        # Import any key which was defined in the old definition, but is missing from the new
        for k in old.keys():
            if k not in self._entry:
                self._entry[k] = old[k]
        
        # Retain special attributes
        oa = set([])
        na = set([])
        if 'attr' in self._entry:
            na = set(self._entry['attr'])
        if 'attr' in old:
            oa = set(old['attr'])
        # Update user-modifiable attributes
        for a in ('ExeSummary', 'ExeEnd', 'ExeAlways', 'Enabled', 'Disabled'):
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
        if nt == 'Table' and 'current' in self._entry:
            new_def = [h[1] for h in self['current'][0]]
            old_def = [h[1] for h in old['current'][0]]
            if new_def != old_def:
                logging.debug('Incompatible table definition', self['handle'], new_def, old_def)
                self._entry['current']=[self['current'][0]]
        
        # No type change: exit
        if ot == nt:
            return
        # Hard-coded 'old' type differs from configured type:
        # Import all special keys that might be defined in old but missing in
        # self
        for k in ['type']+upkeys:
            if k in old:
                self._entry[k] = old[k]
        if 'current' not in self._entry:
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
            # Keep the new current if nothing else is found
            elif 'current' in old:
                oc = old['current']
                if type(nc)!=type(oc):
                    nc = oc
            self._entry['current'] = nc
        except:
            logging.debug('Impossible to migrate current value', old['handle'], nc, ot)
            # Remove current key
            del self._entry['current']
