# -*- coding: utf-8 -*-
"""Options aggregation"""
import collections 
from traceback import format_exc
import re

from misura.canon.logger import get_module_logging
logging = get_module_logging(__name__)

import numpy as np

def aggregate_table(targets, values, devices, precision=[], visible=[]):
    """Calculate the table() aggregate"""
    result = []
    for i, x in enumerate(values[targets[0]]):
        row = []
        for t in targets:
            row.append(values[t][i])
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
        opt = d.gete(t)
        header.append((opt['name'], opt['type']))
        units.append(opt.get('unit', False))
        if i>=N:
            visible.append(not opt.get('parent', False))
            if opt['type'] in ['Float', 'Integer', 'Number']:
                precision.append(opt.get('precision', 2))
    result = [header] + result
    return result, units, precision, visible


def aggregate_merge_tables(targets, values, current):
    """Calculate the merge_tables() aggregate"""
    all_y = []
    # Discover all possible y values
    key_col = 0
    xlen = 1
    for t in targets:
        for tab in values[t]:
            if len(tab) == 0:
                logging.debug('Zero-length table in merge_tables', t, tab)
                continue
            xlen += len(tab[0]) - 1 - key_col
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
    result = [current[0]] + result
    return result

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
        return function_name+'()'
    return function_name+'('+','.join(targets)+')'

class Aggregative(object):
    """A configuration object fragment proving aggregate capability"""
    
    def collect_aggregate(self, aggregation, handle=False):
        function_name, targets = decode_aggregation(aggregation)
        if not len(targets) and handle:
            targets = [handle]        
        values = collections.defaultdict(list)
        devices = collections.defaultdict(list)
        fullpaths = collections.defaultdict(list)
        for child in self.devices:
            target = None
            pack = collections.defaultdict(list)
            devpack = collections.defaultdict(list)
            pathpack = collections.defaultdict(list)
            for target in targets:
                # Ensure all targets exist
                if not child.has_key(target):
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
                for t in targets:
                    values[t] += pack[t]
                    fullpaths[t] += pathpack[t]
                    devices[t] += devpack[t]
            else:
                self.log.error('calc_aggregate: no values packed for', child['devpath'], target)

        return function_name, targets, values, fullpaths, devices
    
    def calc_aggregate(self, aggregation, handle=False):
        function_name, targets, values, fullpaths, devices = self.collect_aggregate(aggregation, handle=handle)
        result = None
        error = None       
        # TODO: calc stdev here
        if function_name == 'mean':
            v = np.array(values[targets[0]]).astype(np.float32)
            # FIXME: hack to filter out zeros
            v1 = v[v != 0]
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
        elif function_name == 'table':
            opt = False
            visible = []
            precision = []
            if handle:
                opt = self.gete(handle)
                visible = opt['visible']
                precision =  opt['precision']
            result, units, precision, visible = aggregate_table(targets, values, devices, precision, visible)
            if opt and result:
                opt['unit'] = units
                opt['visible'] = visible
                opt['precision'] = precision
                self.sete(handle, opt)
        elif function_name == 'merge_tables':
            result = aggregate_merge_tables(targets, values, self[handle])
        else:
            self.log.error(
                'Aggregate function not found:', function_name, aggregation)
        return result, error

    def update_aggregates(self, recursive=1):
        """Updates aggregate options. recursive==1, upward; -1, downward; 0, no"""
        # TODO: move to Scriptable class! (or a new one)
        for handle, opt in self.desc.iteritems():
            if not opt.has_key('aggregate'):
                continue
            aggregation = opt['aggregate']
            try:
                result, error = self.calc_aggregate(aggregation, handle)
            except:
                self.log.error('Error during aggregation', self.get_fullpath(),
                              aggregation, handle, format_exc())
                continue
            if result is not None:
                self[handle] = result
                if error is not None and opt.has_key('error'):
                    self[opt['error']] = error
            else:
                self.log.error('Aggregation failed for ', self.get_fullpath(),
                               handle, aggregation)
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
            self.log.debug('Aggregation target already found:', opt, target, old)
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
            self.log.debug('Aggregation target already missing:', opt, target, old)
            return False
        targets.remove(target)
        new = encode_aggregation(function_name, targets)
        self.setattr(opt, 'aggregate', new)
        self.log.debug('Removed aggregation target:', opt, target, new)
        return True

