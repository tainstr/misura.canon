#!/usr/bin/python
# -*- coding: utf-8 -*-
import functools
import re

from ..logger import get_module_logging
logging = get_module_logging(__name__)
navigator_domains = []

#FIXME: hacks!
isDataset = lambda ds: (hasattr(ds, 'dstype') and hasattr(ds, 'datatype'))
QtUserRole = 32

def docname(ds):
    """Get dataset name by searching in parent document data"""
    for name, obj in ds.document.data.iteritems():
        if obj == ds:
            return name
    return None


def node(func):
    """Decorator for functions which should get currentIndex node if no arg is passed"""
    @functools.wraps(func)
    def node_wrapper(self, *a, **k):
        n = False
        keyword = True
        # Get node from named parameter
        if k.has_key('node'):
            n = k['node']
        # Or from the first unnamed argument
        elif len(a) >= 1:
            n = a[0]
            keyword = False
        # If node was not specified, get from currentIndex
        if n is False:
            n = self.model().data(self.currentIndex(), role=QtUserRole)
        elif isDataset(n):
            n = docname(n)

        # If node was expressed as/converted to string, get its corresponding
        # tree entry
        if isinstance(n, str) or isinstance(n, unicode):
            logging.debug('%s %s', 'traversing node', n)
            n = str(n)
            n = self.model().tree.traverse(n)

        if keyword:
            k['node'] = n
        else:
            a = list(a)
            a[0] = n
            a = tuple(a)
        logging.debug(
            '%s %s %s %s', '@node with', n, type(n), isinstance(n, unicode))
        return func(self, *a, **k)
    return node_wrapper


def nodes(func):
    """Decorator for functions which should get a list of currentIndex nodes if no arg is passed"""
    @functools.wraps(func)
    def node_wrapper(self, *a, **k):
        n = []
        keyword = True
        # Get node from named parameter
        if k.has_key('nodes'):
            n = k['nodes']
        # Or from the first unnamed argument
        elif len(a) >= 1:
            n = a[0]
            keyword = False
        # If node was not specified, get from currentIndex
        if not len(n):
            n = []
            for idx in self.selectedIndexes():
                n0 = self.model().data(idx, role=QtUserRole)
                n.append(n0)
        if keyword:
            k['nodes'] = n
        else:
            a = list(a)
            a[0] = n
            a = tuple(a)
        logging.debug(
            '%s %s %s %s %s', '@nodes with', n, type(n), isinstance(n, unicode))
        return func(self, *a, **k)
    return node_wrapper


class NavigatorDomain(object):

    def __init__(self, navigator):
        self.navigator = navigator

    @property
    def model(self):
        """Hack to allow nodes() decorator"""
        return self.navigator.model

    def currentIndex(self, *a, **k):
        return self.navigator.currentIndex(*a, **k)

    def selectedIndexes(self, *a, **k):
        return self.navigator.selectedIndexesPublic(*a, **k)

    @property
    def mainwindow(self):
        return self.navigator.mainwindow

    @property
    def doc(self):
        return self.navigator.doc

    def xnames(self, *a, **k):
        return self.navigator.xnames(*a, **k)

    def dsnode(self, *a, **k):
        return self.navigator.dsnode(*a, **k)

    def plot(self, *a, **k):
        return self.navigator.plot(*a, **k)

    def is_loaded(self, node):
        return (node.ds is not False) and (len(node.ds) > 0)

    def is_plotted(self, node):
        if not self.is_loaded(node):
            return False
        return len(self.model().is_plotted(node.path)) > 0

    def double_clicked(self, node):
        return False

    def check_node(self, node):
        """Check if node pertain to this domain"""
        return True

    def match_node_path(self, node, rule):
        if (not node) or (not node.path):
            return False
        regex = re.compile(rule.replace('\n', '|'))
        return regex.search(node.path)

    def check_nodes(self, nodes):
        """Check if multiple nodes selection pertain to this domain"""
        return True

    def add_base_menu(self, menu, node=False):
        return True

    def build_base_menu(self, menu, node=False):
        if not self.check_node(node):
            return False
        return self.add_base_menu(menu, node)

    def add_file_menu(self, menu, node):
        return True

    def build_file_menu(self, menu, node):
        if not self.check_node(node):
            return False
        return self.add_file_menu(menu, node)

    def add_group_menu(self, menu, node):
        return True

    def build_group_menu(self, menu, node):
        if not self.check_node(node):
            return False
        return self.add_group_menu(menu, node)

    def add_sample_menu(self, menu, node):
        return True

    def build_sample_menu(self, menu, node):
        if not self.check_node(node):
            return False
        return self.add_sample_menu(menu, node)

    def add_dataset_menu(self, menu, node):
        return True

    def build_dataset_menu(self, menu, node):
        if not self.check_node(node):
            return False
        return self.add_dataset_menu(menu, node)

    def add_derived_dataset_menu(self, menu, node):
        return True

    def build_derived_dataset_menu(self, menu, node):
        if not self.check_node(node):
            return False
        return self.add_derived_dataset_menu(menu, node)

    def add_multiary_menu(self, menu, nodes):
        return True

    def build_multiary_menu(self, menu, nodes):
        if not self.check_nodes(nodes):
            return False
        return self.add_multiary_menu(menu, nodes)
