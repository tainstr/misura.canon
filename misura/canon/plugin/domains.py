#!/usr/bin/python
# -*- coding: utf-8 -*-

navigator_domains = set([])

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
        return self.navigator.selectedIndexes(*a, **k)
    
    @property
    def mainwindow(self):
        return self.navigator.mainwindow
    
    @property
    def doc(self):
        return self.navigator.doc
    
    def xnames(self,*a,**k):
        return self.navigator.xnames(*a,**k)
    
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
        
    def check_node(self, node):
        """Check if node pertain to this domain"""
        return True
    
    def check_nodes(self, nodes):
        """Check if multiple nodes selection pertain to this domain"""
        return True
    
    def add_file_menu(self, menu, node):
        return True
    
    def build_file_menu(self, menu, node):
        if not self.check_node(node):
            return False
        return self.add_file_menu(menu, node)  
    
    def add_sample_menu(self, menu, node):
        return True
    
    def build_sample_menu(self, menu, node):
        if not self.check_node(node):
            return False
        return self.add_sample_menu(menu, node)  
    
    def add_group_menu(self, menu, node):
        return True
    
    def build_group_menu(self, menu, node):
        if not self.check_node(node):
            return False
        return self.add_group_menu(menu, node)  
    
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
    