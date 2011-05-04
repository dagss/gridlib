import os
import sys
from subprocess import Popen, PIPE
import subprocess
import re
from textwrap import dedent
from os.path import join as pjoin
import socket
import time

__all__ = ['LocalSpawnExecutor']

from .executor import DirectoryExecutor, DirectoryFuture

class LocalSpawnExecutor(DirectoryExecutor):
    """DirectoryExecutor subclass that simply fires off another daemon
    Python process on the same machine (so that one can terminate the
    instantiating process without terminating spawned tasks).

    This can be very useful for, e.g., spawning jobs that require
    downloading a resource from the internet, without having to
    reserve a cluster node.

    NOTE: Only works on Posix with Bash currently.
    """

    def _create_jobscript(self, human_name, job_name, work_path):
        jobscriptpath = pjoin(work_path, 'run.sh')
        with file(jobscriptpath, 'w') as f:
            f.write(dedent("""\
            #!/bin/bash
            cd {path}
            echo $$ > pid
            exec 2>&1 >> log </dev/null
            echo $(date) Starting job {job_name}
            {python} <<END
            {script}
            END
            echo $(date) End of job, exit status $?
            """).format(python=sys.executable,
                        path=os.path.realpath(pjoin(self.store_path, job_name)),
                        script=self.get_launching_python_code(job_name),
                        job_name=job_name))

    def _submit_dir(self, job_path):
        scriptfile = pjoin(job_path, 'run.sh')
        pidfile = pjoin(job_path, 'pid')
        subprocess.check_call('bash %s &' % scriptfile, shell=True)
        while not os.path.exists(pidfile):
            time.sleep(0.001)
        with file(pidfile) as f:
            pid = f.read().strip()
        return '%s:%s' % (socket.gethostname(), pid)

