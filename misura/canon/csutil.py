# -*- coding: utf-8 -*-
"""Common utilities between client and server"""
import os
import sys
import numpy
import multiprocessing
try:
    import cPickle as pickle
    import xmlrpclib
    from exceptions import Exception, KeyboardInterrupt
except:
    import pickle
    import xmlrpc as xmlrpclib
    unicode=str


import numpy as np
import functools
import collections
from traceback import format_exc, print_exc

from threading import ThreadError
import inspect
from operator import itemgetter

profiling = True

#############
# TIME SCALING (testing)
###
import time as standardTime
time_scaled = multiprocessing.Value('i')
time_scaled.value = 0
time_factor = multiprocessing.Value('d')
time_factor.value = 1.0


def time():
    """If time_scaled is set, return current simulation-scaled time."""
    if time_scaled.value == 1:
        return time_step() * time_factor.value * 1.
    return float(standardTime.time())
utime = time

def sleep(t):
    return standardTime.sleep(t)

sh_time_step = multiprocessing.Value('i')


def time_step(set=-1):
    if set < 0:
        return sh_time_step.value
    else:
        sh_time_step.value = set
        return set


def doTimeStep(set=-1):
    t = time_step()
    if set >= 0:
        time_step(set=set)
    else:
        t += 1
        time_step(set=t)
    return t


class Void(object):
    pass


class FakeBinary(object):

    """Fake the xmlrpc.Binary behavior when not needing the overhead"""

    def __init__(self, data=''):
        self.data = data
binfunc = FakeBinary


def logprint(*a):
    print(a)


class FakeLogger(object):

    def __getattr__(self, *a):
        return logprint

    def __call__(self, *a, **k):
        print(k, a)
fakelogger = FakeLogger()


def xmlrpcSanitize(val, attr=[], otype=False):
    if isinstance(val, dict):
        r = {}
        for k, v in val.items():
            r[k] = xmlrpcSanitize(v)
        return r
    if hasattr(val, '__iter__') and not isinstance(val, dict):
        r = list(xmlrpcSanitize(el) for el in val)
#       if len(r)==1: return r[0]
        return r
    if type(val) in [type(numpy.float64(0)), type(numpy.float32(0))]:
        return float(val)
    if type(val) in [type(numpy.int64(0)), type(numpy.int32(0)), type(numpy.int8(0))]:
        return int(val)
    if type(val) == type(numpy.string_()):
        return str(val)
    if otype == 'Profile':
        return binfunc(pickle.dumps(val))
    elif ('Binary' in attr):
        return binfunc(val)
    return val


def sanitize(func):
    """XML Sanitizer decorator"""
    @functools.wraps(func)
    def sanitize_wrapper(self, *a, **k):
        r = func(self, *a, **k)
        return xmlrpcSanitize(r)
    # Save original argspec, so it can be retrieved by func_args().
    sanitize_wrapper.wrapped = inspect.getargspec(func)
    return sanitize_wrapper


def func_args(func):
    """Return a flatten list of function argument names.
    Correctly detect decorated functions"""
    if hasattr(func, 'wrapped'):
        w = func.wrapped
        r = w.args
        if w.varargs:
            r.append(w.varargs)
        if w.keywords:
            r.append(w.keywords)
        return r
    # non-wrapped
    if hasattr(func, 'func_code'):
        return func.func_code.co_varnames
    # Non-function?
    return []

#####
# FILESYSTEM UTILS
#######
goodchars = ['-', '_', ' ']
badchars = []


def validate_filename(fn, good=goodchars, bad=badchars):
    """Purifica un filename da tutti i caratteri potenzialmente invalidi"""
    fn = unicode(fn)
    fn = fn.encode('ascii', 'ignore')
    # scarta tutti i caratteri non alfanumerici
    fn = "".join([x for x in fn if x.isalpha() or x.isdigit()
                  or x in goodchars and x not in badchars])
    return fn


def incremental_filename(original):
    if not os.path.exists(original):
        return original
    ext = ''
    base = original.split('.')
    
    if len(base)>1:
        ext = '.'+base.pop(-1)
    base = '.'.join(base)
    prenum = base.split('_')
    number = 0
    if len(prenum)>1:
        try:
            number = int(prenum[-1])+1
            prenum.pop(-1)
        except:
            pass
            
    prenum = '_'.join(prenum)
    
    new = prenum+'_'+str(number)+ext
    while os.path.exists(new):
        number += 1
        new = prenum+'_'+str(number)+ext
        
    return new


def iter_cron_sort(top, field=1, reverse=False):
    """
    Return ordered of tuples (path,ctime,size) for all files from `top` folder recursively and ordered by field.
    field 0: name, 1: time, 2: size
    """
    r = []
    for root, dirs, files in os.walk(top):
        for name in files:
            f = os.path.join(root, name)
            s = os.stat(f)
            r.append((f, s.st_ctime, s.st_size))
    r = sorted(r, key=itemgetter(field), reverse=reverse)
    return r


def disk_free(path, unit=1048576.):
    """Calculate free disk space"""
    st = os.statvfs(path)
    f = st.f_frsize / unit
    free = st.f_bavail * f
    total = st.f_blocks * f
    used = (st.f_blocks - st.f_bfree) * f
    return free, total, used


def chunked_upload(upfunc, fp, sigfunc=lambda v: 0):
    name = os.path.basename(fp)
    w = 2000000
    N = 1. * os.stat(fp).st_size / w
    if N < 1:
        N = 1
    f = open(fp, 'rb')
    pf = 100. / N
    for i in range(int(N)):
        dat = f.read(w)
        upfunc(xmlrpclib.Binary(dat), i, name)
        pc = pf * i
        sigfunc(int(pc))
        print('sent %.2f' % (pc))
    ui = -1
    if N == 1:
        ui = 0
    dat = f.read()
    upfunc(xmlrpclib.Binary(dat), ui, name)
    if N == 1:
        upfunc(xmlrpclib.Binary(''), -1, name)
    sigfunc(100)


def flatten(x):
    """Build a flat list out of any iter-able at infinite depth"""
    result = []
    for el in x:
        # Iteratively call itself until a non-iterable is found
        if hasattr(el, "__len__") and not isinstance(el, str):
            flt = flatten(el)
            result.extend(flt)
        else:
            result.append(el)
    return result


def find_nearest_brute(v, x):
    """`v`: lista o vettore monodimensionale dove ricercare il valore `x`"""
    v = abs(numpy.array(v) - x)
    f = np.where(v == v.min())[0][0]
    return f


def find_nearest_val(v, t, get=False, seed=None):
    """Finds value nearest to `t` in array `v`, assuming v is monotonic.
    Optionally use the `get` function instead of v.__getitem__, in order to retrieve single elements from the array."""
    # TODO: explore builtin bisect module!!!
    if get is False:
        g = v.__getitem__
    else:
        g = get

    if len(v) <= 1:
        print('find_nearest_val: empty arg')
        return 0
    g0 = g(0)
    g_1 = g(-1)
    positive = g_1 > g0
    if (t < g0 and positive) or (t > g0 and not positive):
        return 0
    n = len(v) - 1
    if (t > g_1 and positive) or (t < g_1 and not positive):
        return n
    if seed is None:
        i = len(v) / 2  # starting index
    else:
        i = seed
    smaller = 0  # prev smaller index
    bigger = n  # prev bigger index
    bi = i  # best index
    bd = t * 1000  # min delta
    ok = 2  # ping-pong counter
    while ok > 0:
        c = g(i)        # current delta
        d = t - c       # delta
        b = abs(d)
        # remember best index and delta
        if b < bd:
            bd = b
            bi = i
        # can't be better than that
        if d == 0:
            bi = i
            break
        # bigger d: choose smaller index
        if (d < 0 and positive) or (d > 0 and not positive):
            if i - smaller < 1:
                break
            bigger = i
            # Detect last integer reduction
            if i == smaller + 1:
                i = smaller
                ok -= 1
            else:
                # Half-way reduce towards `smaller`
                i = smaller + (i - smaller) / 2
        # smaller d: choose bigger index
        else:
            if bigger - i < 1:
                break
            smaller = i
            # Detect last integer increase
            if i == bigger - 1:
                i = bigger
                ok -= 1
            else:
                # Half-way increase towards `bigger`
                i += (bigger - i) / 2

    return bi


def toslice(v):
    """Recursive conversion of an iterable into slices"""
    if not isinstance(v, collections.Iterable):
        return v
    r = []
    isSlice = True
    for i, el in enumerate(v):
        if isinstance(el, collections.Iterable):
            el = toslice(el)
            isSlice = False
        if isinstance(el, slice):
            isSlice = False
        r.append(el)
    if isSlice == True:
        return slice(*r)
    return tuple(r)

def decode_cool_event(event):
    """Decode a ">cool,temperature,timeout" thermal cycle event"""
    if not event.startswith('>cool'):
        return False
    event = event.split(',')
    T = float(event[1])
    if len(event)>2:
        timeout = float(event[2])
    return T, timeout

def decode_checkpoint_event(event):
    if not event.startswith('>checkpoint'):
        return False
    event = event.split(',')
    delta = float(event[1])
    if len(event)>2:
        timeout = float(event[2])
    return delta, timeout

def next_point(crv, row, delta=1, events=False):
    """Search next non-event point in thermal cycle curve `crv`.
    Starts by checking `row` index, then adds `delta` (+-1),
    until proper value is found."""
    d = 0
    c = True
    N = len(crv)
    while c and 0 <= row < N:
        ent = crv[row]
        c = isinstance(ent[1], basestring)
        # Decode natural cooling event
        if c and events and ent[1].startswith('>cool'):
            c = False
        if c:
            row += delta
    if row < 0:
        return -1, False
    if row >= N:
        return N, False

    return row, ent

class initializeme(object):
    """Decorator to protect a function call behind an initializing flag"""
    def __init__(self, repeatable=False):
        """`repeatable`=False will not allow the decorated function to be called if the object is 
        already initializing."""
        self.repeatable = repeatable
    
    def __call__(self, func):
        repeatable = self.repeatable
        @functools.wraps(func)
        def initializeme_wrapper(self, *args, **kwargs):
            if self['initializing'] and not repeatable:
                self.log.error('Already initializing: cannot exec', func)
                raise BaseException('Already initializing: cannot execute')
            try:
                self['initializing'] = True
                r = func(self, *args, **kwargs)
                self['initializing'] = False
                return r
            except KeyboardInterrupt:
                raise KeyboardInterrupt()
            except:
                self.log.error('initializing_flag: exc calling', func, args, kwargs, format_exc())
                raise
            finally:
                self['initializing'] = False
            return False
        return initializeme_wrapper

class lockme(object):
    """Decorator to lock/unlock method execution.
    The class having its method decorated must expose
    a _lock object compatible with threading.Lock."""
    def __init__(self, timeout=5):
        self.timeout=timeout
        
    def __call__(self, func):
        timeout = self.timeout
        @functools.wraps(func)
        def lockme_wrapper(self, *args, **kwargs):
            if self._lock is False:
                return func(self, *args, **kwargs)
            if isinstance(self._lock, multiprocessing.synchronize.Lock):
                r = self._lock.acquire(timeout=timeout)
                if not r:
                    raise BaseException("Failed to acquire lock")
            else:
                # Threading locks does not support timeout (py2)
                self._lock.acquire()
            try:
                return func(self, *args, **kwargs)
            except KeyboardInterrupt:
                raise KeyboardInterrupt()
            except:
                print('lockme: exc calling', func, args, kwargs)
                print_exc()
            finally:
                try:
                    self._lock.release()
                except:
                    pass
        return lockme_wrapper

def unlockme(func):
    """Decorator to finally unlock after method execution.
    Useful if locking must be delayed.
    The class having its method decorated must expose
    a _lock object compatible with threading.Lock."""
    @functools.wraps(func)
    def unlockme_wrapper(self, *args, **kwargs):
        if self._lock is False:
            return func(self, *args, **kwargs)
        try:
            return func(self, *args, **kwargs)
        finally:
            try:
                self._lock.release()
            except:
                pass
    return unlockme_wrapper


class retry(object):

    """Decorator class to retry a function execution"""

    def __init__(self, times=None, hook=False):
        self.times = times
        self.hook = hook

    def __call__(self, func):
        @functools.wraps(func)
        def retry_loop(*args, **kwargs):
            # If retry was defined as None,
            # it is expected as a property of the first argument (the func's
            # self).
            retry = self.times
            if retry is None:
                retry = args[0].retry
            times = retry
            retry0 = retry
            while True:
                try:
                    r = func(*args, **kwargs)
                    return r
                except KeyboardInterrupt:
                    raise KeyboardInterrupt()
                except:
                    print('retry', retry0 - retry,  func, times)
                    retry -= 1
                    if retry <= 0:
                        print('End retry')
                        raise
                    # Call the retry hook
                    if self.hook is not False:
                        self.hook(
                            func, args, kwargs,  sys.exc_info(), times - retry)
        return retry_loop




import cProfile
import pstats

profile_path = '/tmp/misura/profile/'


def start_profiler(obj):
    obj.__p = cProfile.Profile()
    obj.__p.enable()


def stop_profiler(obj):
    out = '{}{}_{}.prf'.format(
        profile_path, obj.__class__.__name__, str(id(obj)))
    print('PROFILING STATS FOR', out)
    s = pstats.Stats(obj.__p)
    s.sort_stats('cumulative')
    s.dump_stats(out)
    obj.__p.disable()


prf_uid = -1


def profile(func):
    """Profiling decorator for multiprocessing calls"""
    if not profiling:
        return func

    @functools.wraps(func)
    def profile_wrapper(self, *a, **k):
        global prf_uid
        prf_uid += 1
        p = cProfile.Profile()
        try:
            fp = getattr(self, '__getitem__', {'fullpath': ''}.get)('fullpath')
        except:
            fp = ''
        name = func.__name__
        if isinstance(self, object):
            name = self.__class__.__name__ + '.' + name
        if not os.path.exists(profile_path):
            os.makedirs(profile_path)
        out = '{}{}_{}_{}.prf'.format(profile_path,
                                      name,
                                      fp.replace('/', '_'),
                                      prf_uid)
        print('START PROFILING STATS FOR', func.__name__, ' AT ', repr(self), out)
        p.enable()
        r = func(self, *a, **k)
        s = pstats.Stats(p)
        s.sort_stats('cumulative')
        s.dump_stats(out)
        p.disable()
        print('END PROFILING STATS FOR', func.__name__, ' AT ', repr(self), out)
        return r
    return profile_wrapper


def from_seconds_to_hms(seconds):
    minutes, seconds = divmod(seconds, 60)
    hours, minutes = divmod(minutes, 60)
    return "%d:%02d:%02d" % (hours, minutes, seconds)


def ensure_directory_existence(path):
    directory = os.path.dirname(path)
    if not os.path.exists(directory):
        os.makedirs(directory)

# LOCALE CONTEXT MANAGER
from contextlib import contextmanager
import locale
import threading
LOCALE_LOCK = threading.Lock()

@contextmanager
def setlocale(name):
    """Temporary locale context manager. For datetime.strptime with Qt
    From: http://stackoverflow.com/a/24070673/1645874"""
    with LOCALE_LOCK:
        saved = locale.setlocale(locale.LC_ALL)
        try:
            yield locale.setlocale(locale.LC_ALL, name)
        finally:
            locale.setlocale(locale.LC_ALL, saved)
          

from datetime import datetime  
def decode_datetime(val, format='%a %b %d %H:%M:%S %Y'):
    #Wed Nov 11 18:35:36 2015
    with setlocale('C'):
        ret = datetime.strptime(val, format)
    return ret


def filter_calibration_filenames(filenames):
    return [filename for filename in filenames if not '/calibration/' in filename.lower()]

def only_hdf_files(filenames):
    return [filename for filename in filenames if filename.endswith('.h5')]
