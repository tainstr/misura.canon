# -*- coding: utf-8 -*-
"""Options aggregation"""
import collections
from traceback import format_exc
import re

from misura.canon.logger import get_module_logging
logging = get_module_logging(__name__)

import numpy as np


def aggregate_table(targets, values, devices, tree, precision=[], visible=[], function_name='table'):
    """Calculate the table() aggregate"""
    result = []
    flat = function_name=='table_flat'
    devpaths = []
    for i, x in enumerate(values[targets[0]]):
        row = []
        for j, t in enumerate(targets):
            row.append(values[t][i])
            # Add subtree columns
            if flat:
                for el in tree:
                    row.append(el[j])
        devpaths.append(tree[i][-1])
        result.append(row)
    # Reorder by first column
    result = sorted(result, key=lambda e: e[0])
    # Calculate table properties
    header = []
    units = []
    N = len(precision)
    for i, t in enumerate(targets):
        # Take the first device
        d = devices[t]
        if not len(d):
            logging.error('No device found for target', t)
            return None, None, None, None
        d = d[0]
        # Get the target option
        opt = d.gete(t)
        h = opt.get('column', opt['name'])
        header.append((h, opt['type']))
        units.append(opt.get('unit', False))
        # Calculate visibility and precision
        if i >= N:
            # Extend visibility
            visible.append(True)
            if opt['type'] in ['Float', 'Integer', 'Number']:
                precision.append(opt.get('precision', 2))
        # Hide if has a parent
        v = not opt.get('parent', False)
        # Hide also if the 'error' is found (should use is_error_col...)
        v *= 'error' not in h.lower()
        # Hide also if hidden
        v *= 'Hidden' not in opt['attr']
        visible[i] = v
    
    # Extend attributes if table is flat
    if flat:
        h1,v1,u1,p1 = [],[],[],[]
        
        for i,h in enumerate(header):
            v = visible[i]
            u = units[i]
            p = precision[i]
            h1.append(h)
            v1 += [v]*(len(devpaths)+1)
            u1 += [u]*(len(devpaths)+1)
            p1 += [p]*(len(devpaths)+1)
            
            for d in devpaths:
                h1.append(('{} {}'.format(d, h[0]), h[1]))
            
        header= h1
        units = u1
        visible = v1
        precision = p1
        
        print 'header',header
        print 'visible',visible
        print 'precision',precision
        print 'units',units
 
            
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

    def collect_aggregate(self, aggregation, handle=False):
        function_name, targets = decode_aggregation(aggregation)
        if not len(targets) and handle:
            targets = [handle]
        values = collections.defaultdict(list)
        devices = collections.defaultdict(list)
        fullpaths = collections.defaultdict(list)
        tree = []
        for child in self.devices:
            target = None
            pack = collections.defaultdict(list)
            devpack = collections.defaultdict(list)
            pathpack = collections.defaultdict(list)
            for target in targets:
                # Ensure all targets exist
                if target not in child:
                    self.log.error('calc_aggregate: missing target in child object',
                                   handle, aggregation, child['devpath'], target, targets)
                    # This cell will remain empty
                    pack[target].append(None)
                # Skip this device entirely
                elif child.getattr(target, 'type') == 'RoleIO':
                    self.log.error('calc_aggregate: child object exposes a RoleIO for target',
                                   child['devpath'], target)
                    pack = False
                    break
                else:
                    #Target is fine
                    pack[target].append(child[target])
                devpack[target].append(child)
                pathpack[target].append(child['fullpath'])
                
            if pack:
                subtree = []
                for t in targets:
                    values[t] += pack[t]
                    fullpaths[t] += pathpack[t]
                    devices[t] += devpack[t]

                    opt = child.gete(t)
                    if 'tree' in opt:
                        subtree.append(opt['tree'])
                    else:
                        subtree.append(child[t])
                subtree.append(child['devpath'])
                tree.append(subtree)
            else:
                self.log.error(
                    'calc_aggregate: no values packed for', child['devpath'], target)

        return function_name, targets, values, fullpaths, devices, tree

    def calc_aggregate(self, aggregation, handle=False):
        function_name, targets, values, fullpaths, devices, tree = self.collect_aggregate(
            aggregation, handle=handle)
        result = None
        error = None
        # TODO: calc stdev here
        if function_name == 'mean':
            v = np.array(values[targets[0]]).astype(np.float32)
            # FIXME: hack to filter out zeros and nans
            v1 = v[v != 0]
            v1 = v1[np.isfinite(v1)]
            if len(v1):
                result = float(v1.mean())
                error = float(v1.std())
            else:
                self.log.debug('calc_aggregate: Zero-length', aggregation, v)
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
            result, units, precision, visible = aggregate_table(
                targets, values, devices, tree, precision, visible, function_name)
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
        return result, error, tree

    def update_aggregate(self, handle):
        """Update aggregation for option `handle`"""
        opt = self.gete(handle)
        if 'aggregate' not in opt:
            raise RuntimeError(
                'Cannot update: option has no aggregate: ' + handle)
        aggregation = opt['aggregate']
        result, error, tree = self.calc_aggregate(aggregation, handle)
        if result is not None:
            self[handle] = result
            self.setattr(handle, 'tree', tree)
            if error is not None and 'error' in opt:
                self[opt['error']] = error
        else:
            self.log.error('Aggregation failed for ', self.get_fullpath(),
                           handle, aggregation)
            return False
        return True

    def update_aggregates(self, recursive=1):
        """Updates aggregate options. recursive==1, upward; -1, downward; 0, no"""
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
            self.parent().update_aggregates(recursive=1)
        elif recursive < 0:
            for k in self.children.keys():
                self.child(k).update_aggregates(recursive=-1)
            # Then run backwards as aggregates only propagates bottom-up
            if self.parent():
                self.parent().update_aggregates(recursive=1)

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
