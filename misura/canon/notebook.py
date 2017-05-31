# -*- coding: utf-8 -*-
"""
Short demo for Notebook functionality. Targets: 
0. Print test info
1. Show an image by index
2. Show an image by T
3. Print characteristic shapes
4. Default Plot Vol/T
"""
from misura.canon.indexer import SharedFile
from misura.canon import reference
from IPython.display import Image, display, HTML
import numpy as np
import logging
from matplotlib import pylab
logging.getLogger().setLevel(55)

# To display an image:
# http://stackoverflow.com/questions/11854847/display-an-image-from-a-file-in-an-ipython-notebook

def render_meta(obj):
    out = '<table>\n<tr><th>Name</th><th>Temperature</th><th>Time</th></tr>'
    keys = []
    for key, opt in obj.desc.items():
        if opt['type']!='Meta':
                continue
        keys.append(key)
    keys.sort()
    for key in keys:
        opt = obj.desc[key]
        m = opt['current']
        ok = True
        for k, v in m.items():
            if not isinstance(v, str):
                f = '{:.1f}'
                if k=='time':
                    v/=60.
                    f += ' min'
                elif k=='temp':
                    f+= ' °C'
                if k=='value':
                    m[k] = '{:E}'.format(v)
                else:
                        m[k] = f.format(v)
            elif k=='time' and v in ['None', None]:
                ok = False
        if not ok:
            continue
        out += '<tr><td>{}</td><td>{}</td><td>{}</td></tr>\n'.format(opt['name'], m['temp'], m['time'])
    out += '</table>\n'
    return out
            
class NodeAccessor(object):
    _node = False
    def __init__(self, sharedfile, path='/'):
        self._sh = sharedfile
        self._path = path
        self._local = ['_sh', '_path', '_local'] + dir(self)
        
    @property 
    def node(self):
        if self._node is False:
            self._node = self._sh.test.get_node(self._path)
        return self._node
        
    def __getattr__(self, subpath):
        if subpath.startswith('_') or subpath in self._local:
            return object.__getattribute__(self, subpath)
        if subpath not in self.node:
            return object.__getattribute__(self, subpath)
           
        if not self._path.endswith('/'):
            s = self._path+'/'+subpath
        else: 
            s = self._path + subpath
        return NodeAccessor(self._sh, s)
    
    def __call__(self):
        p = reference.get_node_reference(self._sh, self._path)
        return p
    
    def __getitem__(self, *a, **k):
        return self().__getitem__(*a, **k)
        
    def get_profile(self, idx=None, T=None, t=None):
        if not 'profile' in self.node:
            print('This node has no profile dataset')
            return False
        p = reference.get_node_reference(self._sh, self._path+'/profile')
        if T is not None:
            idx, t, val = self._sh.nearest(self._path+'/T', T)
        if t is not None:
            idx = self._sh.get_time_cumulative_profile(self._path+'/profile', t)
        if idx is not None:
            dat = p[idx]
        return dat
        
    def draw_profile(self, idx=None, T=None, t=None):
        dat = self.get_profile(idx=idx, T=T, t=t)
        if not dat:
            return False
        t, ((h, w), x, y) = dat
        y -= min(y)
        y = max(y)-y
        x -= min(x)
        pylab.plot(x, y)
        m = max((max(x),max(y)))
        pylab.ylim((-50, m))
        pylab.xlim((-50, m))
        pylab.show()
        return True
                
            

class MisuraFile(SharedFile):
    def __init__(self, *a, **k):
        SharedFile.__init__(self, *a, **k)
        self.load_conf()
        self.nb = NodeAccessor(self)

    def info(self):
        """Pretty-print main test info"""
        ins = self.conf.instrument_obj
        out = '<h1>Name: {}</h1>\n'.format(ins.measure['name'])
        out+= '<h3> Started: {}, Elapsed: {} min</h3>\n'.format(ins.measure['date'], ins.measure['elapsed']/60)
        out += '<h2> Measurement metadata </h2>\n'
        out += render_meta(ins.measure)
        out += '<h2>  Sample metadata for: {} </h2>\n'.format(ins.sample0['name'])
        out += render_meta(ins.sample0)
        display(HTML(out))
        

