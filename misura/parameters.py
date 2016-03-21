#!/usr/bin/python
# -*- coding: utf-8 -*-
"""Basic parameters and data structures"""
import os
import sys
from commands import getoutput as go

version = 0  # Misura version

# LOGIN
useAuth = True  # Richiedi autenticazione.
exclusiveLogin = False  # Può accedere al server un solo utente per volta.
loginDuration = 20  # Persistenza del login dopo l'ultima operazione. (minuti)
ssl_enabled = True

# Ambiente di debug
debug = True
utest = False  # unittesting environment
simulator = True  # Enable simulation of devices
real_devs = True  # Search for real devices
# +1        # utilizza multithreading durante l'inizializzazione
init_threads = False
# non cercare i controlli possibili sulle telecamere (+ veloce, solo debug)
no_cam_controls = False
announceZeroconf = False  # annuncia il server con zeroconf
parallel_computing = True
managers_cache = 10
dummy_cameras = 2  # init dummy cameras (num >=0)

def determine_path(root=__file__):
    """Borrowed from wxglade.py"""
    try:
        #       root = __file__
        if os.path.islink(root):
            root = os.path.realpath(root)
        return os.path.dirname(os.path.abspath(root))
    except:
        print "I'm sorry, but something is wrong."
        print "There is no __file__ variable. Please contact the author."
        sys.exit()
# PATHS
home = os.path.expanduser("~") + '/'
mdir = determine_path()  # Executable path
mdir += '/'  # Tutti i percorsi sono indicati con / finale.
misuraServerExe = mdir + 'MisuraServer.pyc'
baseStorageDir = home + 'storage/'  # Persistenza dati e configurazioni
baseTmpDir = '/tmp/misura/'
# {}/'.format(getpass.getuser())            # RAM filesystem for performance
baseRunDir = '/dev/shm/misura/'
testdir = mdir + 'tests/'  # Test automatizzati


# Detect hardware mac address of this machine
HW = go("ifconfig | grep 'HW' | awk '{ print $5}'|head -n1")
# Check if netbooted - setup default dir in /opt/machines
NETBOOTED = go('df -h|grep "/$"|grep ":/var/deploy" >/dev/null; echo $?')
if NETBOOTED == '0':
    NETBOOTED = True
    baseStorageDir = "/opt/machines/" + HW + '/'

# LOGGING
log_basename = 'misura.log'
log_format = '%(levelname)s:%(asctime)s %(message)s'
log_disk_space = 1000 * (10 ** 6)  # Bytes (1GB)
log_file_dimension = 10 * (10 ** 6)  # Bytes (10MB)

# PORTS (only for standalone test instances)
main_port = 3880

# FILES
ext = '.h5'  # Misura test file extension
conf_ext = '.csv'
curve_ext = '.crv'
characterization_ext = '.csv'

fileTransferChunkLimit = 2  # MB
min_free_space_on_disk = 500  # MB
# lines. Max buffer length in per-device acquisition cycles
buffer_length = 100
buffer_dimension = 10 ** 6
MAX = 2 ** 32
MIN = 10 ** -10

# CAMERA ID
multisample_processing_pool = True
netusbcam = False +1
xiapi = False # +1
video4linux = True  # +1
cameraCaps = {'default': {'format': 'video/x-raw-rgb', 'width': 640, 'height': 480},
              # Logitech C910
              'UVC Camera (046d:0821)': {'format': 'video/x-raw-yuv', 'width': 2048, 'height': 1536},
              # Logitech C310
              'UVC Camera (046d:081b)': {'format': 'video/x-raw-yuv', 'width': 1280, 'height': 960}
              #     'UVC Camera (046d:0821)':{'format':'video/x-raw-yuv', 'width':1920, 'height':1080}
              }


forbiddenID = ['Laptop_Integrated_Webcam_3M']

# SERIAL DEVICES
max_serial_scan = 5
enable_EurothermTr = True  # -1
enable_Eurotherm_ePack = True  # -1
enable_DatExel = True  # -1
enable_PeterNorbergStepperBoard = True  # -1
enable_TC08 = True -1

#######
# LOGIC #####################
#######
from multiprocessing import Value

ERRVAL = 2.**127
#######
# TESTING
#######
testing = False  # Impostato automaticamente dai test che lo richiedono

# Se in unittesting, non utilizzare funzionalità twisted

log_backup_count = int(1.*log_disk_space / log_file_dimension) + 1

# DERIVED PATHS
defaults = baseStorageDir
webdir = baseStorageDir + 'web/'


# Will be redefined later
storagedir = baseStorageDir
confdir = storagedir
datadir = storagedir + 'data/'
tmpdir = baseTmpDir
rundir = baseRunDir
logdir = datadir + 'log/'
log_filename = logdir + log_basename


def create_dirs(vd):
    for d in vd:
        if not os.path.exists(d):
            print 'Creating directory:', d
            os.makedirs(d)
# Per generare certificati nuovi:
# openssl genrsa 2048 > privkey.pem
# openssl req -new -x509 -sha512 -key privkey.pem -out cacert.pem -days 0


def set_confdir(cf):
    global confdir
    confdir = cf
    create_dirs([confdir])
    print 'set confdir', cf


def set_datadir(dd):
    global datadir, logdir, log_filename
    datadir = dd
    logdir = datadir + 'log/'
    log_filename = logdir + log_basename
    create_dirs([datadir, logdir])
    print 'set datadir', dd


def set_rundir(rd):
    global rundir
    rundir = rd
    create_dirs([rundir])
    print 'set rundir', rd


def regenerateDirs():
    """Redefine and regenerate directories"""
    global confdir, datadir, logdir, log_filename, rundir
    confdir = storagedir
    vd = [tmpdir, rundir, tmpdir + 'profile']
    datadir = storagedir + 'data/'
    vd.append(datadir)
    logdir = datadir + 'log/'
    vd.append(logdir)
    log_filename = logdir + log_basename
    create_dirs(vd)


def setInstanceName(name=False):
    global storagedir, tmpdir, rundir
    if name:
        # FIXME: re-test, very old code! Must adapt also other dirs?
        storagedir += name + '/'
        tmpdir += name + '/'
        rundir += name + '/'
        regenerateDirs()

regenerateDirs()
