# Functions that can be used from test script (and are in a package)
from joblib.hpc import versioned, fileref
import urllib
import os

@versioned(deps=False)
def func1(x, y):
    return x**2 + y

class MyException(Exception):
    pass

@versioned(deps=False)
def funcex(x, y):
    raise MyException()


## def download_monitor(bcount, bsize, totalsize):
##     'show current download progress'
##     if bcount == 0:
##         download_monitor.percentage_last_shown = 0.
##     bytes = bcount * bsize
##     percentage = bytes * 100. / totalsize
##     if percentage >= 10. + download_monitor.percentage_last_shown:
##         logger.info('downloaded %s bytes (%2.1f%%)...'
##                     % (bytes, percentage))
##         download_monitor.percentage_last_shown = percentage



@versioned(1, deps=False)
def download_url(url, targetfile):
    t = urllib.urlretrieve(url, targetfile)
    return fileref(targetfile)
