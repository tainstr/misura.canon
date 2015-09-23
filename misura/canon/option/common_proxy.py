# -*- coding: utf-8 -*-
"""Option persistence."""

def from_column(column, proxy):
	"""Returns the object able to return the column `col` and the column option name"""
	column_without_prefix = column.split(':')
	column_without_prefix = column_without_prefix[0] if len(column_without_prefix) == 1 else column_without_prefix[1]

	if column_without_prefix.startswith('/' + 'summary' + '/'):
	    column_without_prefix = column_without_prefix[9:]

	v = column_without_prefix.split('/')
	if v[0] == '':
	    v.pop(0)
	name = v.pop(-1)
	obj = proxy.toPath(v)
	return obj, name