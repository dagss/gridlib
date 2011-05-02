"""Test DirectoryExecutor
"""

from concurrent.futures import ProcessPoolExecutor
import logging
import tempfile
import shutil
import os
import contextlib
import functools
from os.path import join as pjoin
from nose.tools import ok_, eq_, assert_raises

from ..executor import DirectoryExecutor, DirectoryFuture, execute_directory_job
from .. import versioned
from ...numpy_pickle import load, dump


# Debug settings
KEEPSTORE = False
# To see log during debugging, use 'nosetests --nologcapture'

#
# Create mock executors that simply forwards to ProcessPoolExecutor
# HOWEVER, communication doesn't happen the normal way, but
# rather through the disk-pickles of DirectoryExecutor. The
# only direct IPC is the path to the job directory as a string.
#

class MockExecutor(DirectoryExecutor):
    configuration_keys = DirectoryExecutor.configuration_keys + (
        'before_submit_hook',)

    default_before_submit_hook = False
    default_poll_interval = 1e-1
    
    def __init__(self, *args, **kw):
        DirectoryExecutor.__init__(self, *args, **kw)
        self.subexecutor = ProcessPoolExecutor()
        self.given_work_paths = []
        self.submit_count = 0

    def _create_future_from_job_dir(self, job_name):
        return MockFuture(self, job_name)

    def _create_jobscript(self, human_name, job_name, work_path):
        self.given_work_paths.append(work_path)
        with file(pjoin(work_path, 'jobscript'), 'w') as f:
            f.write('jobscript for job %s\n' % human_name)


class MockFutureError(Exception):
    pass

class MockFuture(DirectoryFuture):
    subfuture = None # stays None for cached jobs
    
    def _submit(self):
        if self._executor.before_submit_hook:
            self._executor.before_submit_hook(self)
        self._executor.submit_count += 1
        self._executor.logger.debug('Submitting job')
        procex = self._executor.subexecutor
        assert isinstance(self.job_path, str)
        self.subfuture = procex.submit(execute_directory_job, self.job_path)
        return 'job-%d' % (self._executor.submit_count - 1)


#
# Test utils
#

@contextlib.contextmanager
def working_directory(path):
    old = os.getcwd()
    try:
        os.chdir(path)
        yield
    finally:
        os.chdir(old)

def ls(path='.'):
    return set(os.listdir(path))

def ne_(a, b):
    assert a != b, "%r == %r" % (a, b)

def filecontents(filename):
    with file(filename) as f:
        return f.read()

@contextlib.contextmanager
def function_replaced_in_process(sourcename, targetname):
    """
    Temporarily replace a function in this module so that it
    pickles/unpickles
    """
    source = globals()[sourcename]
    target = globals()[targetname]
    source_name = source.__name__
    try:
        source.__name__ = target.__name__
        globals()[targetname] = source
        yield
    finally:
        globals()[targetname] = target
        source.__name__ = source_name

#
# Test context
#

def with_store():
    def with_store_dec(func):
        @functools.wraps(func)
        def inner():
            global store_path, logger
            store_path = tempfile.mkdtemp(prefix='jobstore-')
            logging.basicConfig()
            logger = logging.getLogger()
            logger.setLevel(logging.DEBUG)
            logger.info('JOBSTORE=%s' % store_path)
            try:
                for x in func():
                    yield x
            finally:
                if KEEPSTORE:
                    shutil.rmtree(store_path)
        return inner
    return with_store_dec

@versioned(1, deps=False)
def func(x, y):
    return x + y

@with_store()
def test_basic():
    "Basic tests"
    tests = []
    PRE_SUBMIT_LS = set(['input.pkl', 'jobscript'])
    COMPUTED_LS = PRE_SUBMIT_LS.union(['output.pkl', 'jobid', 'log'])
    
    def before_submit(fut):
        with working_directory(fut.job_path):
            tests.append((eq_, ls(), PRE_SUBMIT_LS))
            input = load('input.pkl')
            tests.append((eq_, input, dict(
                args=(1, 1),
                func=func,
                version_info=func.version_info,
                kwargs={})))
            tests.append((eq_, 'jobscript for job func\n',
                          filecontents('jobscript')))
    
    executor = MockExecutor(store_path=store_path,
                            logger=logger,
                            before_submit_hook=before_submit)

    # Run a single job, check that it executes, and check input/output
    fut = executor.submit(func, 1, 1)    
    yield eq_, executor.submit_count, 1
    yield eq_, fut.result(), 2
    yield ne_, executor.given_work_paths[0], fut.job_path
    with working_directory(fut.job_path):
        output = load('output.pkl')
        yield eq_, output, ('finished', 2)
        yield eq_, ls(), COMPUTED_LS
        yield eq_, len(executor.given_work_paths), 1
        yield eq_, filecontents('jobid'), 'job-0\n'

    # Re-run and check that result is loaded from cache
    fut = executor.submit(func, 1, 1)
    yield eq_, fut.result(), 2
    yield eq_, executor.submit_count, 1

    # Run yet again with different input
    executor.before_submit_hook = lambda x: None
    fut2 = executor.submit(func, 1, 2)
    yield eq_, fut2.result(), 3
    yield eq_, executor.submit_count, 2
    yield ne_, fut2.job_path, fut.job_path

    # Run tests queued by closures
    yield eq_, len(tests), 3
    for x in tests:
        yield x
    
class MyException(Exception):
    pass

@versioned(1, deps=False)
def throws(x, y):
    raise MyException('message %s %s' % (x, y))

@with_store()
def test_exception():
    "Exception propagation"
    executor = MockExecutor(store_path=store_path,
                            logger=logger)
    fut = executor.submit(throws, 'a', 'b')
    yield assert_raises, MyException, fut.result
    yield eq_, str(fut.exception()), 'message a b'
    

@versioned(2, deps=False)
def func_2(x, y):
    return x + y + 1

@with_store()
def test_different_versions():
    "Difference in source code versions causes errors"
    # We replace the func function in the current process; but
    # spawning another process will get the original func. So we try
    # to compute a job using the "wrong source".  Guard against this
    # situation (e.g., user did not push source code to cluster, and
    # submitted job from laptop).
    global func

    executor = MockExecutor(store_path=store_path,
                            logger=logger)

    assert func is not func_2
    fut = executor.submit(func, 1, 1)
    yield eq_, fut.result(), 2
    print fut.job_path
    
    with function_replaced_in_process('func_2', 'func'):
        assert func is func_2
        fut = executor.submit(func, 1, 1)

    yield ok_, fut.subfuture is not None

    # Trap the expected error from the IPC subfuture.
    # For a concrete cluster implementation the effect is that
    # "the job dies", and the exception is found in the logs.
    try:
        print fut.subfuture.result()
    except RuntimeError, e:
        yield (eq_, str(e),
               'Source revision mismatch: Submitted job with '
               'version 2 of func, but available source has version 1')
    else:
        yield ok_, False, 'Source mismatch did not cause error'

