import os
from textwrap import dedent
from os.path import join as pjoin

from .. import DirectoryExecutor, DirectoryFuture
        
class SlurmExecutor(DirectoryExecutor):
    configuration_keys = DirectoryExecutor.configuration_keys + (
        'account', 'nodes', 'mem_per_cpu', 'time',
        'tmp', 'ntasks_per_node', 'omp_num_threads',
        'sbatch_command_pattern')
    default_nodes = 1
    default_mem_per_cpu = '2000M'
    default_time = '01:00:00' # max run time
    default_tmp = '100M' # scratch space
    default_omp_num_threads = '$SLURM_NTASKS_PER_NODE'
    default_ntasks_per_node = 1

    # Configurable by base classes for specific clusters
    pre_command = ''
    post_command = ''
    python_command = 'python'

    def _slurm(self, scriptfile):
        cmd = "sbatch '%s'" % scriptfile
        if os.system(cmd) != 0:
            raise RuntimeError('command failed: %s' % cmd)
    
    def get_launch_command(self, job_name):
        return dedent("""\
        {python} <<END
        {script}
        END
        """).format(python=self.python_command,
                    script=self.get_launching_python_code(job_name))

    def _create_jobscript(self, human_name, job_name, work_path):
        jobscriptpath = pjoin(work_path, 'sbatchscript')
        script = make_slurm_script(
            jobname=human_name,
            command=self.get_launch_command(job_name),
            logfile=pjoin(self.store_path, job_name, 'log'),
            precmd=self.pre_command,
            postcmd=self.post_command,
            queue=self.account,
            ntasks=self.ntasks_per_node,
            nodes=self.nodes,
            openmp=self.omp_num_threads,
            time=self.time)
        with file(jobscriptpath, 'w') as f:
            f.write(script)

    def _create_future_from_job_dir(self, job_path):
        return SlurmFuture(self, job_path)
        
class SlurmFuture(DirectoryFuture):
    def _submit(self):
        scriptfile = pjoin(self.job_path, 'sbatchscript')
        jobid = self._executor._slurm(scriptfile)
        return jobid
        
def make_slurm_script(jobname, command, logfile, where=None, ntasks=1, nodes=None,
                      openmp=1, time='24:00:00', constraints=(),
                      queue='astro', precmd='', postcmd='',
                      mpi=None):
    settings = []
    for c in constraints:
        settings.append('--constraint=%s' % c)
    if nodes is not None:
        if ntasks % nodes != 0:
            raise ValueError('ntasks=%d not divisible by nnodes=%d' % (ntasks, nodes))
        settings.append('--ntasks-per-node=%d' % (ntasks // nodes))
        settings.append('--nodes=%d' % nodes)
        if mpi is None:
            mpi = True
    else:
        settings.append('--ntasks=%d' % ntasks)
        if mpi is None:
            mpi = ntasks > 1
    lst = '\n'.join(['#SBATCH %s' % x for x in settings])
    if where is None:
        where = '$HOME'
    mpicmd = 'mpirun ' if mpi else ''
    
    template = dedent("""\
        #!/bin/bash
        #SBATCH --job-name={jobname}
        #SBATCH --account={queue}
        #SBATCH --time={time}
        #SBATCH --mem-per-cpu=2000
        #SBATCH --tmp=100M
        #SBATCH --output={logfile}
        {lst}
        
        source /site/bin/jobsetup
        export OMP_NUM_THREADS={openmp}
        source $HOME/masterenv

        cd {where}
        {precmd}
        {mpicmd}{command}
        {postcmd}
        """)
    return template.format(**locals())
