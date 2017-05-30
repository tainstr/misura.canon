# -*- coding: utf-8 -*-
"""Generalized logging utilities"""
import os
import logging
import functools
from datetime import datetime
from . import csutil


def unicode_func(value, **k):
    try:
        return str(value, **k)
    except TypeError:  # Wasn't a bytes object, no need to decode
        return str(value)
    
try:
    unicode('a')
    unicode_func=unicode
except:
    unicode = str
    basestring= str
    

    
root_log = logging.getLogger()
root_log.setLevel(-1)
formatter = logging.Formatter("%(levelname)s: %(asctime)s %(message)s")
for h in root_log.handlers:
    h.setFormatter(formatter)
def concatenate_message_objects(*msg):
    # Ensure all message tokens are actually strings
    # (avoid "None" objects pollute the buffer!)
    msg = list(msg)
    for i, e in enumerate(msg):
        if isinstance(e, unicode):
            continue
        elif isinstance(e, basestring):
            msg[i] = unicode_func(e, errors='ignore')
        else:
            msg[i] = unicode_func(repr(e), errors='ignore')
    return msg
    
def formatMsg(*msg, **po):
    """Format the message for pretty visualization"""
    t = csutil.time()
    st = datetime.fromtimestamp(t).strftime('%x %X.%f')
    # Owner e priority
    o = po.get('o')
    p = po.get('p')
    pid =po.get('pid', os.getpid())
    if p == None:
        p = logging.NOTSET
    if o == None or o == '':
        own = ' '
        o = ''
    elif pid:
        own = ' (%s%i): ' % (o, pid)
    else:
        own = ' (%s): ' % (o)
    msg = concatenate_message_objects(*msg)
    smsg = ' '.join(tuple(msg))
    smsg = smsg.splitlines()
    pmsg = '%s%s' % (own, smsg[0])
    if len(smsg) > 1:
        for l in smsg[1:]:
            pmsg += '\n\t | %s' % l
    return t, st, p, o, msg, pmsg


def justPrint(*msg, **po):
    t, st, p, o, msg, pmsg = formatMsg(*msg, **po)
    print(st+' '+pmsg)


def toLogging(*msg, **po):
    """Send log to standard python logging library"""
    t, st, p, o, msg, pmsg = formatMsg(*msg, **po)
    logging.log(po.get('p', 10), pmsg)



class BaseLogger(object):

    """Interface for standard logging functions definition and routing."""

    def __init__(self, log=justPrint):
        self.log = log

    def __call__(self, *msg, **po):
        return self.log(*msg, p=po.get('p', logging.DEBUG))

    def debug(self, *msg):
        return self.log(*msg, p=logging.DEBUG)

    def info(self, *msg):
        return self.log(*msg, p=logging.INFO)

    def warning(self, *msg):
        return self.log(*msg, p=logging.WARNING)

    def error(self, *msg, **o):
        return self.log(*msg, p=logging.ERROR)

    def critical(self, *msg):
        return self.log(*msg, p=logging.CRITICAL)


class SubLogger(BaseLogger):

    """Implicit owner logging."""

    def __init__(self, parent):
        # TODO: pass DirShelf option path as log, instead of using a
        # parent.desc.
        BaseLogger.__init__(self, self.log)
        self.parent = parent

    def log(self, *msg, **po):
        p = po.get('p', 0)
        msg = list(msg)
        for i, e in enumerate(msg):
            msg[i] = unicode_func(str(e), errors='ignore')
        smsg = u' '.join(tuple(msg))
        if self.parent and self.parent.desc:
            self.parent.desc.set_current('log', [p, smsg])
        return smsg

def get_module_logging(owner):
    logfunc = functools.partial(toLogging, o=owner, pid=False)
    r = BaseLogger(log=logfunc)
    return r

global Log, log
Log = BaseLogger(log=toLogging)
log = Log.log

