# Author: Dag Sverre Selejbotn <d.s.seljebotn@astro.uio.no>
# Copyright (c) 2011 Dag Sverre Seljebotn
# License: BSD Style, 3 clauses.

from os.path import join as pjoin
import tempfile
import shutil
import os
from nose.tools import ok_, eq_, assert_raises
from time import sleep
import functools
from functools import partial
from pprint import pprint

from joblib.job_store import (COMPUTED, MUST_COMPUTE, WAIT, IllegalOperationError,
                              DirectoryJobStore, get_job_store)


from ..cluster_job_store import *
from ..versioned import versioned

tempdir = store = None

def setup_store(**kw):
    global tempdir, store
    tempdir = tempfile.mkdtemp()
    store = ClusterJobStore(tempdir, save_npy=True, mmap_mode=None, **kw)
                                          
def teardown_store():
    global tempdir, store
    shutil.rmtree(tempdir)
    tempdir = store = None
    
def with_store(**kw):
    def with_store_dec(func):
        @functools.wraps(func)
        def inner():
            setup_store(**kw)
            try:
                for x in func():
                    yield x
            finally:
                teardown_store()
        return inner
    return with_store_dec


@versioned(deps=False)
def f(x, y=2):
    return x + y

def find(path):
    result = []
    for dirpath, dirnames, filenames in os.walk(path):
        dirnames.sort() # affects recursion
        filenames.sort()
        result.extend(pjoin(dirpath[len(path):], f).replace(os.sep, '/') for f in filenames)
    return result

def listeq_(a, b, msg=None):
    assert len(a) == len(b), msg or "list lengths differ: %d vs %d" % (len(a), len(b))
    for x, y in zip(a, b):
        assert x == y, msg or "%r != %r" % (x, y)


@with_store()
def test_basic():
    a = store.get_job(f, dict(x=2))
    b = store.get_job(f, dict(x=2))
    yield eq_, a.load_or_lock(), (MUST_COMPUTE, None)
    yield eq_, b.load_or_lock(blocking=False), (WAIT, None), 'a'
    # Not transactional, so next statement 'commits'
    a.persist_output((2, 3, 4))
    a.persist_input((2,), {}, dict(x=2))
    a.commit()
    yield eq_, b.load_or_lock(blocking=False), (COMPUTED, (2, 3, 4)), 'b'
    a.close()
    b.close()
    fp = ('/jobs/gridlib/test/test_cluster_job_store/f/'
         'lg5idzsnyj3gaaenzvnpay5cpgjv424j')
    jp = fp + '/e6xiwih7iyb36ofbfsw37eif5h5fdrhd'
    yield listeq_, find(tempdir), [
        jp + '/input.pkl',
        jp + '/input_args.json',
        jp + '/output.pkl']

