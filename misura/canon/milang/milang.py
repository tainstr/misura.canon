#!/usr/bin/python
# -*- coding: utf-8 -*-
"""Misura Language or Mini Language. 
Secure minimal Python language subset for conditional evaluation of numerical datasets."""

import ast
from traceback import print_exc, format_exc
from time import time
from .env import BaseEnvironment, ExitException
from .objenv import InterfaceEnvironment, InstrumentEnvironment, KilnEnvironment
from .validator import Validator

try:
    unicode('a')
except:
    unicode=str


class MiLang(object):

    """Validation and execution of a script."""
    whitelist = ['len', 'min', 'max', 'range', 'abs', 'print', 'isinstance']
    """Allowed callable object names."""
    blacklist = ['exec', 'open', 'os', 'path', 'exit',
                 'sys', 'self', 'compile', 'getattr', 'setattr']
    """Forbidden callable objects."""
    error = ""
    """Former compilation error."""
    error_line = -1
    """Line where the error was found."""
    error_col = -1
    """Column where the error begins."""
    code = False
    """Codice compilato pronto all'esecuzione"""
    tree = False
    """AST tree computed from the code"""
    env = BaseEnvironment()
    """Data execution environment"""
    obj_env = InterfaceEnvironment()
    """Output interface execution environment (sample or measurement)."""
    ins_env = InstrumentEnvironment()
    """Current instrument execution environment"""
    kiln_env = KilnEnvironment()
    """Kiln execution environment"""
    measure_env = InterfaceEnvironment()
    """Measure metadata environment"""
    script_env = InterfaceEnvironment()
    """Script originating environment (for accessing parameters, etc)"""
    last = 0.
    """Last time this script was executed"""
    period = 0
    """Execution period"""
    handle = False
    """Handle of the option hosting this scipt"""
    meta = False
    """Metadata option, where to record the output point"""

    def __init__(self, script, env=False, obj_env=False, script_env=False):
        if env:
            self.env = env
            self.prefix = getattr(env, 'prefix', '')
        if obj_env:
            self.obj_env = obj_env
        if script_env:
            self.script_env = script_env
        self.set_script(script)

    def set_script(self, script):
        val, tree = self.validate(script)
        if val:
            self.script = script
            self.tree = tree
            self.code = compile(tree, "<string>", "exec")
            return True
        else:
            self.code = False
            self.tree = False
            print('Validation failed:', self.error)
            return False

    def val_name(self, fname):
        if fname.startswith('_') or fname.endswith('_') or fname in self.blacklist:
            return False
        return True

    def validate(self, script):
        """Script validation. Returns True if the compilation is enabled,
        False if it is forbidden."""
#		self.whitelist=self.env.whitelist+self.obj_env.whitelist+self.ins_env.whitelist
        whitetree = {'names': self.whitelist,
                     'mi': {'names': set(self.env.whitelist)},
                     'obj': {'names': set(self.obj_env.whitelist)},
                     'ins': {'names': set(self.ins_env.whitelist)},
                     'kiln': {'names': set(self.kiln_env.whitelist)},
                     'measure': {'names': set(self.measure_env.whitelist)},
                     'script': {'names': set(self.script_env.whitelist)}}
        # Something strange passed...
        if not (isinstance(script, str) or isinstance(script, unicode)):
            print('Wrong instance passed', type(script))
            return False, False
        c = ast.parse(script)
        validator = Validator(whitetree, self.blacklist)
        try:
            validator.visit(c)
            ok = True
        except:
            self.error = validator.error
            self.error_line = validator.error_line
            self.error_col = validator.error_col
            ok = False
            print_exc()
        return ok, c

    def set_env_outFile(self, hdf):
        self.env.hdf = hdf
        self.ins_env.hdf = hdf
        self.obj_env.hdf = hdf
        self.measure_env.hdf = hdf
        self.kiln_env.hdf = hdf
        self.script_env.hdf = hdf

    def do(self):
        """Execute the code"""
        t = time()
        if (self.period is not None) and (t - self.last < self.period):
            print('Not executing!')
            return False
        self.last = time()
        self.env._reset()
        mi = self.env
        obj = self.obj_env
        ins = self.ins_env
        measure = self.measure_env
        kiln = self.kiln_env
        script = self.script_env
        for env in (mi, obj, ins, measure, kiln, script):
            env.handle = self.handle
        # Definizione degli ambienti subordinati (sample, kiln, etc)
        for s, e in self.env.sub.items():
            m = "%s=mi.%s" % (s, e)
            exec(m)
        if self.code:
            try:
                exec(self.code)
            except ExitException:
                return False
            except:
                print('Error in ', self.handle, format_exc(), obj.obj)
                # FIXME
# 				if ins.obj:
# 					ins.obj.log.error('Error in %s %s %s' % (self.handle,format_exc(),obj.obj))
                return False
            self.env = mi
            return True
        else:
            self.error = "Impossible to execute invalid or empty script"
            return False

    def eval(self, out, ins=None):
        """Execute the code and set the output on interface `out`. 
        Optionally make available an additional namespace ins for the calling instrument. """
        self.obj_env.obj = out
        self.ins_env.obj = ins
        if ins is not None:
            # 			self.set_env_outFile(ins.sharedFile)
            self.set_env_outFile(ins.outFile)
        if getattr(ins, 'measure', False) is not False:
            self.measure_env.obj = ins.measure
        if getattr(ins, 'kiln', False) is not False:
            self.kiln_env.obj = ins.kiln
        do = self.do()
        if not do:
            return False
        # Output dictionary
        m = {'temp': 'None', 'time': 'None', 'value': 'None'}
        ok = False
        for k in m.keys():
            v = getattr(self.env, k)
            if v == None:
                continue
            m[k] = v
            ok = True
        if self.meta:
            out[self.meta] = m
        return ok
