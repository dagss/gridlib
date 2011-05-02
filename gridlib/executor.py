"""
Futures-style executor targeted for clusters
"""

import os
import sys
import socket
import errno
import shutil
import tempfile
import time
import base64
from concurrent.futures import Executor, TimeoutError
from textwrap import dedent
from os.path import join as pjoin

from ..func_inspect import filter_args
from ..hashing import NumpyHasher
from .. import numpy_pickle
from . import fileref as fileref_module

__all__ = ['ClusterFuture', 'ClusterExecutor', 'DirectoryExecutor', 'DirectoryFuture']

# Utils
class NullLogger(object):
    def info(self, *args):
        pass
    error = debug = warning = info
null_logger = NullLogger()

def ensuredir(path):
    try:
        os.makedirs(path)
    except OSError, e:
        if e.errno != errno.EEXIST:
            raise

# Process invariants
hostname = socket.gethostname()
pid = os.getpid()

class ClusterFuture(object):
    """
    Note that we do not currently inherit from Future,
    since it contains a certain amount of implementation
    details geared towards thread-safety that we do not
    currently worry about. This should probably change.
    """
    def submit(self):
        raise NotImplementedError()

    def submitted(self):
        raise NotImplementedError()

class ClusterExecutor(object):
    """
    Options for targeting a specific cluster/location are provided
    through subclassing (system, address, etc.), while options that
    affect a single run (number of nodes, queue account, etc.) are
    provided in constructor arguments.
    """
    configuration_keys = ()
    
    def __init__(self, **kw):
        for key in self.__class__.configuration_keys:
            value = kw.get(key, None)
            if value is None:
                value = getattr(self.__class__, 'default_%s' % key, None)
            if value is None:
                raise TypeError('Argument %s not provided' % key)
            setattr(self, key, value)
    
    def submit(self, func, *args, **kwargs):
        if not hasattr(func, 'version_info'):
            raise ValueError('func does not have @versioned decorator')

        # Compute hashes to find target job path
        args_dict = filter_args(func, func.version_info['ignore_args'],
                                *args, **kwargs)
        
        future = self._create_future(func, args, kwargs, args_dict, should_submit=True)
        return future

    def _create_future(self, func, args, kwargs, filtered_args_dict, should_submit):
        raise NotImplementedError()

class DirectoryExecutor(ClusterExecutor):
    configuration_keys = ClusterExecutor.configuration_keys + (
        'store_path', 'poll_interval', 'logger')
    default_store_path = (os.path.realpath(os.environ['JOBSTORE'])
                          if 'JOBSTORE' in os.environ
                          else None)
    default_poll_interval = 5
    default_logger = null_logger

    def _encode_digest(self, digest):
        return base64.b32encode(digest).lower()

    def _create_future(self, func, args, kwargs, filtered_args_dict, should_submit):
        if not func.version_info['ignore_deps']:
            # TODO: Check a map of possible dependencies executed for
            # this run here, in case a depdency just got changed. This
            # should be done by consulting a pickled on-file database
            # AFAICT.
            raise NotImplementedError('Please use ignore_deps for now')

        # Make job_path containing hashes
        h = NumpyHasher('sha1')
        h.hash((func.version_info['digest'], filtered_args_dict))
        job_name = '%s-%s' % (func.__name__,
                              self._encode_digest(h._hash.digest()))
        # Construct job dir if not existing. Remember that we may
        # race for this; if we got to create the directory, we have
        # the lock.
        we_made_it = self._ensure_job_dir(job_name, func, args, kwargs)
        future = self._create_future_from_job_dir(job_name)
        if we_made_it:
            self.logger.info('Created job: %s' % job_name)
            if should_submit:
                jobid = future.submit()
                self.logger.info('Submitted job as %s: %s' % (jobid, job_name))
        else:
            self.logger.info('Job already exists: %s' % job_name)
        return future

    def _ensure_job_dir(self, job_name, func, args, kwargs):
        """
        Returns
        -------

        Bool ``we_made_it`` indicating if the job had to be created;
        if False, somebode else did.
        """
        jobpath = pjoin(self.store_path, job_name)
        if os.path.exists(jobpath):
            return False
        parentpath = os.path.dirname(jobpath)
        ensuredir(parentpath)
        # Guard against crashes & races: Pickle input to temporary
        # directory, then do an atomic rename.
        workpath = tempfile.mkdtemp(prefix='%s-%d-%s-' %
                                    (hostname, pid, os.path.basename(job_name)),
                                    dir=parentpath)
        try:
            # Dump call to file
            call_info = dict(func=func, version_info=func.version_info,
                             args=args, kwargs=kwargs)
            numpy_pickle.dump(call_info, pjoin(workpath, 'input.pkl'))

            # Create job script
            self._create_jobscript(func.__name__, job_name, workpath)

            # Commit: rename directory
            try:
                os.rename(workpath, jobpath)
                we_made_it = True
            except OSError, e:
                if e.errno != errno.EEXIST:
                    raise
                else:
                    # There was a race; that's fine
                    we_made_it = False
        except:
            shutil.rmtree(workpath) # rollback
            raise

        return we_made_it

    def get_launching_python_code(self, job_name):
        fullpath = pjoin(self.store_path, job_name)
        return dedent("""\
        from joblib.hpc.executor import execute_directory_job
        execute_directory_job(\"%s\")
        """ % fullpath.replace('\\', '\\\\').replace('"', '\\"'))

class DirectoryFuture(ClusterFuture):
    """
    Cluster job based on preserving state in a directory in a local
    file system. This is an abstract class meant for subclassing, in
    particular it needs an implementation of ``_is_job_running`` that
    can query the system for whether the job has died or not.
    """
    # TODO: Make store_path a dictionary of host_patterns -> paths,
    # and ensure that unpickling this object on a different
    # host changes self.path accordingly.
    
    def __init__(self, executor, job_name):
        self.job_name = job_name
        self._executor = executor
        self.job_path = os.path.realpath(pjoin(self._executor.store_path, job_name))

    def cancel(self):
        raise NotImplementedError()
    
    def cancelled(self):
        return False
    
    def running(self):
        pass

    def done(self):
        return self._finished() or self.cancelled()
    
    def result(self, timeout=None):
        status, output = self._result(timeout)
        if status == 'exception':
            raise output
        elif status == 'finished':
            return output
        else:
            assert False
    
    def exception(self, timeout=None):
        status, output = self._result(timeout)
        if status == 'exception':
            return output
        elif status == 'finished':
            return None
        else:
            assert False

    def _result(self, timeout=None):
        sleeptime = self._executor.poll_interval
        logger = self._executor.logger
        if timeout is not None:
            sleeptime = min(sleeptime, timeout)
            endtime = time.time() + timeout            
        while True:
            if self._finished():
                return self._load_output()
            logger.debug('Waiting for job (sleeptime=%s): %s', sleeptime, self.job_name)
            if timeout is not None and time.time() >= endtime:
                raise TimeoutError()
            time.sleep(sleeptime)       

    def _finished(self):
        return os.path.exists(pjoin(self.job_path, 'output.pkl'))

    def _load_output(self):
        self._executor.logger.debug('Loading job output: %s', self.job_name)
        return numpy_pickle.load(pjoin(self.job_path, 'output.pkl'))

    def submit(self):
        jobid = self._submit()
        with file(pjoin(self.job_path, 'jobid'), 'w') as f:
            f.write(jobid + '\n')
        with file(pjoin(self.job_path, 'log'), 'w') as f:
            f.write('%s submitted job (%s), waiting to start\n' %
                    (time.strftime('%Y-%m-%d %H:%M:%S'), jobid))
        return jobid

def execute_directory_job(path):
    input = numpy_pickle.load(pjoin(path, 'input.pkl'))
    func, version_info, args, kwargs = [input[x] for x in ['func',
                                                           'version_info',
                                                           'args',
                                                           'kwargs']]
    if version_info != func.version_info:
        raise RuntimeError('Source revision mismatch: Submitted job with version '
                           '%s of %s, but available source has version %s' %
                           (version_info['version'], func.__name__,
                            func.version_info['version']))
    fileref_module.store_path, fileref_module.job_name = (
        os.path.split(os.getcwd()))
    try:
        output = ('finished', func(*args, **kwargs))
    except BaseException:
        output = ('exception', sys.exc_info()[1])
    # Do an atomic pickle; if output.pkl is present then it is complete
    fd, workfile = tempfile.mkstemp(prefix='output.pkl-', dir=path)
    try:
        os.close(fd)
        numpy_pickle.dump(output, workfile)
        os.rename(workfile, pjoin(path, 'output.pkl'))
    except:
        if os.path.exists(workfile):
            os.unlink(workfile)
        raise

