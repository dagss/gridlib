import os
from os.path import realpath, join as pjoin
__all__ = ['fileref']

# Process parameters
job_name = None
store_path = (realpath(os.environ['JOBSTORE'])
              if 'JOBSTORE' in os.environ
              else None)

def fileref(path):
    if job_name is not None:
        print locals(), job_name, store_path
        assert store_path is not None
        assert realpath(store_path) == store_path
        job_path = pjoin(store_path, job_name)
        rp = realpath(path)
        if rp.startswith(job_path):
            # Refers to file within job
            return JobFileRef(job_name, rp[len(job_path) + 1:])
    return AbsoluteFileRef(path)

class FileRef(object):
    def __str__(self):
        return self.abspath()

    def __eq__(self, other):
        if isinstance(other, FileRef):
            other == other.abspath()
        return self.abspath() == other

    def __ne__(self, other):
        return not self == other

    def __hash__(self):
        return hash(self.abspath())

class AbsoluteFileRef(FileRef):
    def __init__(self, path):
        self._path = realpath(path)

    def abspath(self):
        return self._path

    def __repr__(self):
        return '<fileref %s>' % self.abspath()

class JobFileRef(FileRef):
    """The purpose of this class is that if it is unpickled
    in a process with a different job store path, it will
    redirect. We don't need to worry about comparisons
    etc. because they can only work within-process.
    """
    def __init__(self, job_name, relpath):
        self.job_name = job_name
        self._relpath = relpath

    def abspath(self):
        if store_path is None:
            raise RuntimeError('Not enough context to determine absolute path')
        return pjoin(store_path, self.job_name, self._relpath)

    def __repr__(self):
        return '<fileref %s>' % pjoin('$JOBSTORE', self.job_name, self._relpath)
