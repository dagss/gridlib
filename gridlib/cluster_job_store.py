"""Extend joblib's DirectoryJobStore to better fit my purposes.

Some or most of this may be made into pull requests to joblib at some
point.

 - Change to base32 encoding of digests
 - New directory layout
   - Includes function version in hashing
   - Has a directory of hash -> job path

"""

import os
import base64
from os.path import join as pjoin
import errno

from joblib.job_store import (DirectoryJobStore, DirectoryJob, print_logger,
                              ensure_dir)
from joblib.hashing import NumpyHasher
from joblib.func_inspect import get_func_name, get_func_code

IDS_DIR_NAME = 'ids'
JOBS_DIR_NAME = 'jobs'

def encode_digest(digest):
    return base64.b32encode(digest).lower()    

class ClusterJobStore(DirectoryJobStore):
    """
    Directory structure:

     - ids/4a/$jobhash[2:] -> ../jobs/..../$jobhash
     - jobs/package/module/function/$funchash/$jobhash
     - tags/...
    """
    def get_job(self, func, args_dict):
        if not hasattr(func, 'version_info'):
            raise ValueError('func does not have @versioned decorator')

        func_hash = encode_digest(func.version_info['digest'])

        fplst, name = get_func_name(func)
        fplst.append(name)
        func_path = pjoin(*fplst)

        h = NumpyHasher('sha1')
        h.hash((func.version_info['digest'], args_dict))
        job_hash = encode_digest(h._hash.digest())

        return DirectoryJob(self, func,
                            func_path,
                            func_hash,
                            job_hash)

class ClusterJobError(Exception):
    pass

class ClusterJob(DirectoryJob):
    def __init__(self, store, func, func_path, func_hash, job_hash):
        self.store_path = store.store_path
        job_path = pjoin(self.store_path, JOBS_DIR_NAME, func_path,
                         func_hash, job_hash)
        DirectoryJob.__init__(self,
                              job_path=job_path,
                              store=store,
                              func=func, logger=store.logger,
                              save_npy=store.save_npy,
                              mmap_mode=store.mmap_mode)
        self.job_hash = job_hash
        self._jobid_link = pjoin(self.store_path, IDS_DIR_NAME,
                                 self.job_hash[:2], self.job_hash[2:])

    def persist_input(self, args_tuple, kwargs_dict, filtered_args_dict):
        DirectoryJob.persist_input(args_tuple, kwargs_dict, filtered_args_dict)
        call_info = dict(func=self.func, version_info=self.func.version_info,
                         args=args_tuple, kwargs=kwargs_dict)
        numpy_pickle.dump(call_info, pjoin(workpath, 'input.pkl'))

    def clear(self):
        if os.path.exists(self.job_path):
            shutil.rmtree(self.job_path, ignore_errors=True)
        os.unlink(pjoin(self.store_path, IDS_DIR_NAME,
                        self.job_hash[:2], self.job_hash[2:]))

    def load_or_lock(self, blocking=True, pre_load_hook=_noop,
                     post_load_hook=_noop):
        if self.is_computed():
            status, output = DirectoryJob.load_or_lock(blocking,
                                                       pre_load_hook,
                                                       post_load_hook)
            if status == MUST_COMPUTE:
                # This happens on unpickling errors; we
                # fail hard on those instead
                raise ClusterJobError('Could not unpickle: %s/output.pkl' % self.job_path)
            return (status, output)
        else:
            output = None
            # Make output dir -- use this as our lock to figure out
            # whether it is running/dispatched already, or needs
            # to be computed.
            try:
                os.makedirs(self.job_path)
            except OSError, e:
                if e.errno == errno.EEXIST:
                    running = True
                else:
                    raise
            else:
                running = False
                try:
                    os.symlink(self.job_path, self._jobid_link)
                except OSError, e:
                    if e.errno != errno.EEXIST:
                        raise
            if running:
                if not blocking:
                    return (WAIT, None)
                else:
                    # TODO. Keep in mind that we both need to check for
                    # presence of output.pkl, *and* absence of job_path
                    # (=cancelled)
                    raise NotImplementedError()
            else:
                self._work_path = self.job_path
                return (MUST_COMPUTE, None)

    def commit(self):
        self._work_path = None

    def rollback(self):
        # If we made the output directory, we now need to
        # remove it.
        if self._work_path == self.job_path:
            shutil.rmtree(self.job_path)
            try:
                os.unlink(self._jobid_link)
            except OSError, e:
                if e.errno != errno.EEXIST:
                    raise
        self._work_path = None

    def close(self):
        self.rollback()
