#!/usr/bin/python
# -*- coding: utf-8 -*-
"""Plugin utilities"""

from domains import NavigatorDomain, navigator_domains, node, nodes
from dataimport import Converter, create_tree, create_dataset, search_registry, get_converter, convert_file, data_importers
from veusz_utils import veusz_datasetplugins, veusz_toolsplugins

# List of functions which will be executed to update confdb and extend its options
clientconf_update_functions = [] 

# Mapping of instrument:DefaultPlotPlugin names
default_plot_plugins = {} 

load_rules = []