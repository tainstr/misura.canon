# -*- coding: utf-8 -*-
import re

def from_column(column, proxy):
	"""Returns the object able to return the column `col` and the column option name"""
	column = re.sub("^[0-9]+:", '', column)
	column = re.sub("^/summary/", '', column)

	branches = column.split('/')

	name = branches.pop(-1)
	obj = proxy.toPath(branches)

	return obj, name