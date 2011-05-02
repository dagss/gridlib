import re
from subprocess import Popen, PIPE
from .slurm import SlurmExecutor

class TitanOsloExecutor(SlurmExecutor):
    default_sbatch_command_pattern = 'ssh titan.uio.no "bash -ic \'sbatch %s\'"'
    _sbatch_re = re.compile(r'Submitted batch job ([0-9]+)')

    def _slurm(self, scriptfile):
        pp = Popen(['ssh', '-T', 'titan.uio.no'], stdin=PIPE, stderr=PIPE, stdout=PIPE)
        pp.stdin.write("sbatch '%s'" % scriptfile)
        pp.stdin.close()
        #TODO: Could this deadlock when output is larger than buffer size?
        err = pp.stderr.read()
        out = pp.stdout.read()
        pp.stderr.close()
        pp.stdout.close()
        retcode = pp.wait()
        if retcode != 0:
            raise RuntimeError('Return code %d: %s\nError log:\n%s' % (retcode, ' '.join(cmd),
                                                                       err))
        m = self._sbatch_re.search(out)
        if not m:
            raise RuntimeError('Sbatch provided unexpected output: %s' % out)
        return m.group(1)
