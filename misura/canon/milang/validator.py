#!/usr/bin/python
# -*- coding: utf-8 -*-
"""Misura Language or Mini Language. 
Secure minimal Python language subset for conditional evaluation of numerical datasets."""

import ast
import exceptions


class Validator(ast.NodeVisitor):

    """Validator for MiLang language. 
    Raises an exception when an illegal instruction is found"""
    error = False
    error_col = -1
    error_line = -1

    def __init__(self, whitetree, blacklist):
        self.whitetree = whitetree
        self.whitelist = whitetree['names']
        self.namespaces = whitetree.keys()
        self.blacklist = blacklist
        ast.NodeVisitor.__init__(self)

    def val_name(self, fname):
        """Check if a name is blacklisted"""
        if fname.startswith('_') or fname.endswith('_') or fname in self.blacklist:
            return False
        return True

    def set_error(self, node, msg):
        """Set the error message and error position. Raises exception"""
        self.error = msg
        self.error_line = node.lineno
        self.error_col = node.col_offset
        raise exceptions.BaseException(msg)

    def visit_Attribute(self, node):
        """Verify attribute access on legal objects"""
        val = node.value.id
        attr = node.attr
        okattr = False
        okval = False
        for ns, subtree in self.whitetree.iteritems():
            if ns == 'names':
                continue
            if val == ns:
                #				print "Found attr",ns,subtree['names']
                okval = True
                okattr = attr in subtree['names']
        if not (okattr and okval):
            self.set_error(
                node, 'Illegal attribute access: %s, %s' % (val, attr))

    def visit_Assign(self, node):
        """Verify that variable definition is valid"""

        def explode_target(node, lst):
            if isinstance(node, ast.Name):
                lst.append(node.id)
            elif isinstance(node, ast.Tuple) or isinstance(node, ast.List):
                for n in node.elts:
                    lst = explode_target(n, lst)
            return lst

        vals = explode_target(node.value, [])
        for val in vals:
            if not self.val_name(val):
                self.set_error(
                    node, 'Illegal variable definition to name: %r' % val)
                return False
#		val=node.value
#		if isinstance(val,ast.Name):
#			if not self.val_name(val.id):
#				self.set_error(node,'Illegal variable definition to name: %r' % val.id)
#				return False

        tgt = []
        for sub in node.targets:
            tgt = explode_target(sub, tgt)
        self.namespaces += tgt

    def visit_Call(self, node):
        """Verify that function calls are legal."""
        if isinstance(node.func, ast.Attribute):
            if self.val_name(node.func.attr) and node.func.attr in self.whitelist:
                return
            self.visit_Attribute(node.func)
#        	self.set_error(node,'Illegal attribute call: %r %r' % (node.func.value,node.func.attr))
        elif not self.val_name(node.func.id) or node.func.id not in self.whitelist:
            self.set_error(node, 'Illegal function call: %r' % node.func.id)

    def visit_Import(self, node):
        """Block any import statement."""
        self.set_error(node, 'Illegal import: %r' % (node.names))

    def visit_FunctionDef(self, node):
        """Block illegal function definition."""
        if not self.val_name(node.name):
            self.set_error(
                node, "Invalid function definition name: " + node.name)
        else:
            #			print 'Whitelisting function',node.name
            self.whitelist.append(node.name)
            self.whitetree['names'].append(node.name)

    def visit_ClassDef(self, node):
        """Block illegal class definition."""
        if not self.val_name(node.name):
            self.set_error(node, "Invalid class definition name: " + node.name)
        else:
            #			print 'Whitelisting class', node.name
            self.namespaces.append(node.name)
            self.whitetree[node.name] = {'names': []}
