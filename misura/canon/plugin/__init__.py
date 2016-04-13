#!/usr/bin/python
# -*- coding: utf-8 -*-
"""Plugin utilities"""

from domains import NavigatorDomain, navigator_domains, node, nodes
from dataimport import Converter, create_tree, create_dataset, search_registry, get_converter, convert_file, data_importers
from veusz_utils import veusz_datasetplugins, veusz_toolsplugins

additional_client_configurations = {} # will be added to clientconf