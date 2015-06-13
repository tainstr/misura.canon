# -*- coding: utf-8 -*-
"""Generalized logging utilities"""
import os
import logging
from datetime import datetime
import csutil


def formatMsg(*msg,**po):
	"""Format the message for pretty visualization"""
	t=csutil.time()
	st=datetime.fromtimestamp(t).strftime('%x %X.%f')
	# Owner e priority
	o=po.get('o'); p=po.get('p')
	if p==None: p=logging.NOTSET
	if o==None or o=='':
		own=''
		o=''
	else: own=' (%s%i): ' % (o,os.getpid())
	# Ensure all message tokens are actually strings 
	# (avoid "None" objects pollute the buffer!)
	msg=list(msg)
	for i, e in enumerate(msg):
		msg[i]=str(e)
	smsg=' '.join(tuple(msg))
	smsg=smsg.splitlines()
	pmsg= '%s%s' % (own, smsg[0])
	if len(smsg)>1:
		for l in smsg[1:]:
			pmsg+= '\n\t | %s' % l
	return t,st,p,o,msg,pmsg

def justPrint(*msg,**po):
	t,st,p,o,msg,pmsg=formatMsg(*msg,**po)
	print pmsg
	
def toLogging(*msg, **po):
	"""Send log to standard python logging library"""
	logging.log(po.get('p', 10), *msg)



class BaseLogger(object):
	"""Interface for standard logging functions definition and routing."""
	def __init__(self, log=justPrint):
		self.log=log
	def __call__(self, *msg, **po):
		self.log(*msg, p=po.get('p',logging.DEBUG))	
	def debug(self, *msg):
		self.log(*msg, p=logging.DEBUG)
	def info(self, *msg):
		self.log(*msg,p=logging.INFO)
	def warning(self, *msg):
		self.log(*msg, p=logging.WARNING)
	def error(self, *msg, **o):
		self.log(*msg,p=logging.ERROR)
	def critical(self, *msg):
		self.log(*msg,p=logging.CRITICAL)
		
class SubLogger(BaseLogger):
	"""Implicit owner logging."""
	def __init__(self, parent):
		#TODO: pass DirShelf option path as log, instead of using a parent.desc.
		BaseLogger.__init__(self, self.log)
		self.parent=parent
	def log(self,*msg,**po):
		p=po.get('p',0)
		msg=list(msg)
		for i, e in enumerate(msg):
			msg[i]=unicode(str(e), errors='ignore')
		smsg=u' '.join(tuple(msg))
		self.parent.desc.set_current('log',[p,smsg])
		return p,smsg
		

global Log,log
Log=BaseLogger(log=toLogging)
#Log=BaseLogger(log=justPrint)
log=Log.log

