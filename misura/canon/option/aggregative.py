# -*- coding: utf-8 -*-
"""Options aggregation"""
import collections
from traceback import format_exc
import re

from misura.canon.logger import get_module_logging
logging = get_module_logging(__name__)

import numpy as np

def calc_aggregate_subelements(targets, values, tree):
    devpaths = []
    elements = []
    for i in xrange(len(values[targets[0]])):
        subelements = []
        subdev = []
        for target in targets:
            if len(tree[target])<i+1:
                logging.error('Could not find target object', target, i,tree[target], values[targets[0]])
                continue
            # Retrieve corresponding subaggregation for target and row
            subtree = tree[target][i] 
            # Require unary sub-aggregation
            if len(subtree[1])>1:
                logging.error('Excluding multiary aggregation from table_flat', subtree.keys())
                subelements.append([])
                continue
            # Second level of aggregation
            subelem = list(subtree[1].values())[0]
            subelements.append(subelem) 
            subdev.append([el[-1] for el in subelem])
        elements.append(subelements)
        devpaths.append(subdev)
            
    return elements, devpaths
     
def remove_empty_columns(result):
    if not len(result):
        return result
    rcol = []
    for i in range(len(result[0])):
        col = [row[i] for row in result]
        if set(col)==set([None]):
            rcol.append(i)
    # Remove columns in reversed order
    for r in rcol[::-1]:
        for i, row in enumerate(result):
            row.pop(r)
            result[i] = row 
    return result   
     
     
def aggregate_table(targets, values, devices, tree, precision=[], visible=[], function_name='table', readLevel=6):
    """Calculate the table() aggregate"""
    result = []
    flat = function_name=='table_flat'
    devpaths = []
    elements = []
    
    # Determine aggregate subelements for table flattening
    if flat:
        elements, devpaths = calc_aggregate_subelements(targets, values, tree)
    
    for i, x in enumerate(values[targets[0]]):
        row = []
        for j, t in enumerate(targets):
            # i=row, j=column=t
            v = values[t][i]
            d = devices[t][i]
            if d is None:
                logging.debug('Not collecting', t, i, v, devices[t][i])
                continue
            
            row.append(v)
            # Add subtree columns
            if flat:
                # Retrieve corresponding subaggregation for target and row
                subtree = tree[t][i] 
                # Require unary sub-aggregation
                if len(subtree[1])>1:
                    logging.debug('Excluding multiary aggregation from table_flat', subtree.keys())
                    continue
                # Second level of aggregation
                subelem = elements[i][j]
                for el in subelem: 
                    #print('Appending flat subelements', t, 'j', j, 'sub', el[0], 'el', el)
                    row.append(el[0])
                # Normalize target length
                N = max([len(rowel[j]) for rowel in elements])                
                m = len(subelem)
                if m<N:
                    row += [None]*(N-m)
                
        result.append(row)
        
    # Reorder by first column
    result = sorted(result, key=lambda e: e[0])
    
    ############
    # Calculate table properties
    header = []
    units = []
    precision = []
    visible = []
    
    for i, t in enumerate(targets):
        # Take the first device
        devs = devices[t]
        if not len(devs):
            #logging.debug('No device found for target', t)
            continue
            #return None, None, None, None
        # Get the target option
        opt = None
        for d in devs:
            if d is None:
                continue
            if t in d:
                opt = d.gete(t)
                break
        if not opt:
            #logging.debug('No device found with target', t, [d['fullpath'] for d in devs])
            continue
        
        h = opt.get('column', opt['name'])
        header.append((h, opt['type']))
        units.append(opt.get('unit', False))
        if opt['type'] in ['Float', 'Integer', 'Number']:
            precision.append(opt.get('precision', 2))
        # Calculate visibility and precision
        # Hide if has a parent
        #v = not opt.get('parent', False)
        v = True
        # Hide also if the 'error' is found (should use is_error_col...)
        v *= 'error' not in h.lower()
        # Hide also if hidden
        attr = opt['attr']
        v *= ('Hidden' not in attr) and ('ClientHide' not in attr)
        v *= readLevel>=opt.get('readLevel',-1)
        #print('visible for', i, t, v, 'error' not in h.lower(), ('Hidden' not in attr), ('ClientHide' not in attr), attr, readLevel>=opt.get('readLevel',-1), d['fullpath'])
        visible.append(bool(v))
    
    # Extend attributes if table is flat
    if flat:
        h1,v1,u1,p1 = [],[],[],[]
        
        for i,h in enumerate(header):
            v = visible[i]
            u = units[i]
            p = precision[i]
            h1.append(h)
            v1.append(v)
            # Collect all devpaths for header target `i`
            dp = [d[i] for d in devpaths]
            # Take maximum length
            n = max([len(d) for d in dp])
            # Subordered columns are not visible by default
            v1  += [False]*n
            u1 += [u]*(n+1)
            p1 += [p]*(n+1)
            # Arbirtarily take one longest target to extend the header
            for d in dp:
                if len(d)==n:
                    longest = d
            for d in longest:
                h1.append(('{} {}'.format(h[0], d), h[1]))
            
        header= h1
        units = u1
        visible = v1
        precision = p1
        

    result = remove_empty_columns(result)
                
    #print('aggregate_table', result, units, precision, visible)
    result = [header] + result
    return result, units, precision, visible


def aggregate_merge_tables(targets, values, devices):
    """Calculate the merge_tables() aggregate"""
    all_y = []
    header = [False]
    unit = ['None']
    precision = [0]
    visible = [True]
    # Discover all possible y values
    key_col = 0
    xlen = 1
    for t in targets:
        ds = devices[t]
        for i, tab in enumerate(values[t]):
            d = ds[i]
            opt = d.gete(t)
            dname = d['name']
            if len(tab) == 0:
                logging.debug('Zero-length table in merge_tables', t, tab)
                continue

            h = tab[0][key_col+1:]
            # Set first header, unit
            header[0] = tab[0][key_col]
            # Append other headers
            header += [(dname + '\n' + v[0], v[1]) for v in h]
            # Get first precision, unit
            n = [False] * len(tab[0])
            p = opt.get('precision', n)
            if p and p!='None':
                precision[0] = p[key_col]
                precision += p[key_col+1:]
            u = opt.get('unit', n)
            if u and u!='None':
                unit[0] = u[key_col]
                unit += u[key_col+1:]
            v = opt.get('visible', n)
            if v and v!='None':
                visval = 'Hidden' not in opt['attr']
                if not visval:
                    visible += [visval]*len(v[key_col+1:])
                else:
                    visible += v[key_col+1:]

            xlen += len(h) 
            all_y += [row[key_col] for row in tab[1:]]

    all_y = list(set(all_y))
    all_y.sort()
    # Table template
    result = []
    for y in all_y:
        result.append([0] * xlen)
        result[-1][0] = y
    # X cursor
    xpos = 1
    # Fill the table template
    for t in targets:
        for tab in values[t]:
            if len(tab) == 0:
                continue
            row = []
            for row in tab[1:]:
                iy = all_y.index(row[key_col])
                # Assign values starting from the second column
                for ix, val in enumerate(row[key_col + 1:]):
                    result[iy][xpos + ix] = val
            # Move the X cursor
            xpos += len(row) - 1 - key_col
    result = [header] + result
    
    return result, unit, precision, visible


def decode_aggregation(aggregation):
    function_name = re.search("(.+?)\(", aggregation).group(1)
    targets = re.search("\((.+?)\)", aggregation)
    if targets is None:
        targets = []
    else:
        targets = targets.group(1).replace(' ', '').split(',')
    return function_name, targets


def encode_aggregation(function_name, targets=[]):
    if not targets:
        return function_name + '()'
    return function_name + '(' + ','.join(targets) + ')'


class Aggregative(object):
    """A configuration object fragment proving aggregate capability"""
    _readLevel = 6

    def collect_aggregate(self, aggregation, handle=False):
        function_name, targets = decode_aggregation(aggregation)
        if not len(targets) and handle:
            targets = [handle]
        values = collections.defaultdict(list)
        devices = collections.defaultdict(list)
        fullpaths = collections.defaultdict(list)
        subtree = collections.defaultdict(list)
        
        # Non-recursive aggregations
        if function_name in ('deviation',):
            fp = self['fullpath']
            for t in targets:
                values[t] = [self[t]]
                fullpaths[t] = [fp]
                devices[t] = [self]
                subtree[t].append([self[t], {t:[]}, self['devpath']])
            return function_name, targets, values, fullpaths, devices, dict(subtree)
        
        for child in self.devices:
            target = None
            pack = collections.defaultdict(list)
            devpack = collections.defaultdict(list)
            pathpack = collections.defaultdict(list)
            found = False
            for target in targets:
                # Ensure all targets exist
                if target not in child:
                    #self.log.error('calc_aggregate: missing target in child object',
                    #               handle, aggregation, child['devpath'], target, targets)
                    # This cell will remain empty
                    pack[target].append(None)
                # Skip this device entirely
                elif child.getattr(target, 'type') == 'RoleIO':
                    #self.log.error('calc_aggregate: child object exposes a RoleIO for target',
                    #               child['devpath'], target)
                    pack = False
                    break
                else:
                    #Target is fine
                    pack[target].append(child[target])
                    found = True
                if found:
                    devpack[target].append(child)
                    pathpack[target].append(child['fullpath'])
                else:
                    devpack[target].append(None)
                    pathpack[target].append(None)                 
                
            if pack:

                for t in targets:
                    values[t] += pack[t]
                    fullpaths[t] += pathpack[t]
                    devices[t] += devpack[t]
                    
                    if not t in child:
                        continue
                    
                    opt = child.gete(t)
                    if 'tree' in opt:
                        subtree[t].append(opt['tree']) 
                        #print('Found an aggregate', t, subtree[t][-1])
                    else:
                        # If the option is not an aggregate itself, create an empty subtree
                        subtree[t].append([child[t], {t:[]}, child['devpath']])
            else:
                pass
        return function_name, targets, values, fullpaths, devices, dict(subtree)

    def calc_aggregate(self, aggregation, handle=False):
        function_name, targets, values, fullpaths, devices, subtree = self.collect_aggregate(
            aggregation, handle=handle)
        result = None
        error = None
        # TODO: calc stdev here
        if function_name == 'mean':
            v = np.array(values[targets[0]]).astype(np.float32)
            if self.getattr(handle,'type')=='Boolean':
                result = np.all(v)
            else:  
                # filter out zeros
                v1 = v[v != 0]
                # filter out nans
                v1 = v[np.isfinite(v)]
                if len(v1):
                    result = float(v1.mean())
                    error = float(v1.std())
                else:
                    self.log.debug('calc_aggregate: Zero-length', aggregation, v)
        # % deviation of first argument towards second argument
        elif function_name == 'deviation':
            assert len(targets)==2
            value, reference = values[targets[0]][0], values[targets[1]][0]
            if reference>0:
                result = 100.*(value-reference)/reference
        elif function_name == 'sum':
            result = float(
                np.array(values[targets[0]]).astype(np.float32).sum())
        elif function_name == 'prod':
            result = float(
                np.array(values[targets[0]]).astype(np.float32).prod())
        elif function_name in ('table', 'table_flat'):
            opt = False
            visible = []
            precision = []
            if handle:
                opt = self.gete(handle)
                visible = opt.get('visible', visible)
                precision = opt.get('precision', precision)
            result, units, precision, visible = aggregate_table(targets, values, devices, subtree, 
                                                                precision, visible, function_name, self._readLevel)
            if opt and result:
                opt['unit'] = units
                opt['visible'] = visible
                opt['precision'] = precision
                self.sete(handle, opt)
        elif function_name == 'merge_tables':
            result, unit, precision, visible = aggregate_merge_tables(targets, values, devices)
            opt = False
            if handle and result:
                opt = self.gete(handle)
                opt['unit'] = unit
                opt['visible'] = visible
                opt['precision'] = precision    
                self.sete(handle, opt)
        else:
            self.log.error(
                'Aggregate function not found:', function_name, aggregation)
        return result, error, subtree

    def update_aggregate(self, handle):
        """Update aggregation for option `handle`"""
        opt = self.gete(handle)
        if 'aggregate' not in opt:
            raise RuntimeError(
                'Cannot update: option has no aggregate: ' + handle)
        aggregation = opt['aggregate']
        result, error, subtree = self.calc_aggregate(aggregation, handle)
        if result is not None:
            self[handle] = result
            self.setattr(handle, 'tree', [result, subtree, self['devpath']])
            if error is not None and 'error' in opt and opt['error'] in self:
                if not self.hasattr(opt['error'], 'aggregate'):
                    # Assign only if the destination is not an aggregate itself
                    self[opt['error']] = error
            #self.log.debug('Updated aggregate', handle, result, error)
        else:
            #self.log.error('Aggregation failed for ', self.get_fullpath(),
            #               handle, aggregation)
            return False
        return True

    def update_aggregates(self, recursive=1, stop=False):
        """Updates aggregate options. recursive==1->upward; -1->downward; 0->no;
        stop->stop recursion at specified ConfigurationProxy object"""
        # TODO: move to Scriptable class! (or a new one)
        for handle, opt in self.desc.items():
            if 'aggregate' not in opt:
                continue
            aggregation = opt['aggregate']
            try:
                self.update_aggregate(handle)
            except:
                self.log.error('Error during aggregation', self.get_fullpath(),
                               aggregation, handle, format_exc())
                continue
        if recursive > 0 and self.parent():
            if self.parent() == stop:
                return self
            self.parent().update_aggregates(recursive=1)
        elif recursive < 0:
            last = False
            for k in self.children.keys():
                last = self.child(k).update_aggregates(recursive=-1)
            # Then run backwards as aggregates only propagates bottom-up
            if last and last.parent():
                last.parent().update_aggregates(recursive=1, stop=self)
        return self

    def add_aggregation_target(self, opt, target):
        old = self.getattr(opt, 'aggregate')
        function_name, targets = decode_aggregation(old)
        if target in targets:
            self.log.debug(
                'Aggregation target already found:', opt, target, old)
            return False
        targets.append(target)
        new = encode_aggregation(function_name, targets)
        self.setattr(opt, 'aggregate', new)
        self.log.debug('Added aggregation target:', opt, target, new)
        return True

    def remove_aggregation_target(self, opt, target):
        old = self.getattr(opt, 'aggregate')
        function_name, targets = decode_aggregation(old)
        if target not in targets:
            self.log.debug(
                'Aggregation target already missing:', opt, target, old)
            return False
        targets.remove(target)
        new = encode_aggregation(function_name, targets)
        self.setattr(opt, 'aggregate', new)
        self.log.debug('Removed aggregation target:', opt, target, new)
        return True
