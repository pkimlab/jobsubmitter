""".

.. todo::

    Use a MySQL database if you ever want to run on SciNet.

    ``SELECT ... WHERE idx = xxx FOR UPDATE;``

"""
import os
import os.path as op
import time
import json
import atexit
import logging
import paramiko
import contextlib
import pandas as pd
from . import parse_connection_string

logger = logging.getLogger(__name__)


class JobSubmitter:
    """.

    .. note::

        While it would be safer call the Python executable directly,
        this does not seem to be supported on PBS (at least not on banting :()).
    """

    def __init__(
        self,
        job_name,
        connection_string,
        log_root_path=None,
        force_new_folder=True,
        *,
        concurrent_job_limit=None,
        queue='medium',
        nproc=1,
        walltime='02:00:00',  # hh:mm:ss
        mem=None,
        vmem=None,
        email='noname@example.com',
        email_opts='a',
        qsub_shell='/bin/bash',  # Python does not work on Banting
        qsub_script=op.abspath(op.join(op.dirname(__file__), 'scripts', 'qsub.sh')),
        qsub_script_args={},
        env=None
    ):
        """.

        Parameters
        ----------
        job_name : str
            Name of the job.
        head_node_ip : str
            IP address of the head node.
        head_node_type : str
            Job manager type ['sge', 'pbs', ...]
        log_root_path : str, default None
            Location where the log files should be saved.
            A new folder will be created here for each job.
            TODO: Allow this to be a database.
        force_new_job : bool
            A.
        """
        # TODO: Add more options to the connecton string (username / pass)
        _db_info = parse_connection_string(connection_string)
        head_node_type = _db_info['db_type']
        head_node_ip = _db_info['host_ip']

        # Required arguments
        self.job_name = job_name
        self.head_node_ip = head_node_ip

        if head_node_type not in ['sge', 'pbs']:
            raise ValueError("Wrong 'head_node_type': '{}'".format(head_node_type))
        self.head_node_type = head_node_type

        if log_root_path is None:
            log_root_path = op.join(op.expanduser('~'), 'pbs-output')
        else:
            log_root_path = op.abspath(log_root_path)
        os.makedirs(log_root_path, exist_ok=True)

        log_path = op.join(log_root_path, job_name)
        try:
            os.mkdir(log_path)
        except FileExistsError:
            if force_new_folder:
                logger.error("Each job needs to create it's own empty log folder!")
                raise
            else:
                logger.warning("Using an existing folder for log output. This is dangerous!!!")
        time.sleep(1)
        self.log_path = log_path

        self.ssh = None

        # Default arguments
        self.concurrent_job_limit = concurrent_job_limit
        self.queue = queue
        self.nproc = nproc
        self.walltime = walltime
        self.mem = mem  # '10G'
        self.vmem = vmem  # '12G'
        self.email = email
        self.email_opts = email_opts
        self.qsub_shell = qsub_shell  # Python does not work on Banting
        self.qsub_script = qsub_script
        self.qsub_script_args = qsub_script_args  # key-value dict
        if env is None:
            env = {}
        self.env = {
            **env,
            'SYSTEM_COMMAND': '{system_command}',
            'STDOUT_LOG': op.join(self.log_path, '{job_id}.out'),
            'STDERR_LOG': op.join(self.log_path, '{job_id}.err'),
        }

    # === Manage connection ===

    @contextlib.contextmanager
    def connect(self):
        """Open connection to head node."""
        if self.ssh is not None:
            logger.info("Already connected!")
        else:
            self._connect()
        yield
        self._disconnect()

    def _connect(self):
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(self.head_node_ip)
        self.ssh = ssh
        atexit.register(self._disconnect)

    def _disconnect(self):
        """Close connection to head node."""
        self.ssh.close()
        self.ssh = None
        atexit.unregister(self._disconnect)

    # === Format qsub command ===

    @property
    def qsub_system_command_template(self):
        """Template of the system command which will submit the given job on the head node."""
        # Global options are inside one set of curly braces,
        # local options are inside two sets of curly braces.
        #
        return """\
qsub -S {qsub_shell} -N {job_name} -M {email} -m {email_opts} -o /dev/null -e /dev/null \
{{nproc}}{{walltime}}{{mem}}{{vmem}} {{env_string}} \
'{qsub_script}' {{qsub_script_args_string}} \
"""

    def _format_global(self, template):
        return template.format(**self.__dict__)

    def _format_local(self, template):
        if self.head_node_type == 'sge':
            cluster_opts = dict(
                nproc=' -pe smp {}'.format(self.nproc),
                walltime=' -l h_rt={}'.format(self.walltime),
                mem=' -l mem_free={}'.format(self.mem) if self.mem else '',
                vmem=' -l h_vmem={}'.format(self.vmem) if self.vmem else '',
                env_string=' '.join('-v {}="{}"'.format(*x) for x in self.env.items()),
                qsub_script_args_string=' '.join(
                    '--{} {}'.format(key, value) for key, value in self.qsub_script_args.items()
                ) if self.qsub_script_args else ''
            )
        elif self.head_node_type == 'pbs':
            cluster_opts = dict(
                nproc='-l nodes=1:ppn={}'.format(self.nproc),
                walltime=',walltime={}'.format(self.walltime),
                mem=',mem={}'.format(self.mem) if self.mem else '',
                vmem=',vmem={}'.format(self.vmem) if self.vmem else '',
                env_string='-v ' + ','.join('{}="{}"'.format(*x) for x in self.env.items()),
                qsub_script_args_string='-F "{}"'.format(' '.join(
                    '--{} {}'.format(key, value) for key, value in self.qsub_script_args.items()
                )) if self.qsub_script_args else ''
            )
        else:
            raise Exception
        return template.format(**cluster_opts)

    # === Submit job ===

    def submit(self, iterable):
        results = []
        _system_command_template = (
            self._format_local(self._format_global(self.qsub_system_command_template))
        )
        for i, (job_id, system_command) in enumerate(iterable):
            if self.head_node_type == 'pbs' and ',' in system_command:
                raise Exception(
                    "Can't have commas in 'system_command' when using 'pbs'.\n{}"
                    .format(system_command))
            self._respect_concurrent_job_limit(i)
            system_command = _system_command_template.format(
                job_id=job_id,
                system_command=system_command
            )
            logger.debug(system_command)
            stdout, stderr = self._exec_system_command(system_command)
            time.sleep(0.05)
            results.append((stdout, stderr, ))
        return results

    def get_num_running_jobs(self):
        system_command = 'qstat -u "$USER" | grep "$USER" | grep -i " r  " | wc -l'
        stdout, stderr = self._exec_system_command(system_command)
        num_running_jobs = int(stdout)
        return num_running_jobs

    def get_num_submitted_jobs(self):
        system_command = 'qstat -u "$USER" | grep "$USER" | wc -l'
        stdout, stderr = self._exec_system_command(system_command)
        num_submitted_jobs = int(stdout)
        return num_submitted_jobs

    def _exec_system_command(self, system_command):
        n_tries = 0
        stdout = 'x.x'
        stderr = 'x.x'
        while n_tries < 5 and stderr:
            if n_tries:
                delay = (1 * n_tries)
                logger.debug('Sleeping for {} seconds...'.format(delay))
                time.sleep(delay)
            n_tries += 1
            stdin_fh, stdout_fh, stderr_fh = self.ssh.exec_command(system_command, get_pty=True)
            stdout = stdout_fh.read().decode().strip()
            if stdout:
                logger.debug(stdout)
            stderr = stderr_fh.read().decode().strip()
            if stderr:
                logger.error(stderr)
        return stdout, stderr

    def _respect_concurrent_job_limit(self, i):
            # Limit the number of jobs running simultaneously
            STEP_SIZE = 50
            DELAY = 120
            if self.concurrent_job_limit is not None and ((i + 1) % STEP_SIZE) == 0:
                num_submitted_jobs = self.get_num_submitted_jobs()
                while (num_submitted_jobs + STEP_SIZE) > self.concurrent_job_limit:
                    logger.info(
                        "'concurrent_job_limit' reached! Sleeping for {:.0f} minutes..."
                        .format(DELAY / 60))
                    time.sleep(DELAY)
                    num_submitted_jobs = self.get_num_submitted_jobs()

    # === Job status ===

    def job_status(self, iterable):
        results_all = []
        for job_id, system_command in iterable:
            results = {'job_id': job_id, 'status': None, '~system_command': system_command}
            results_all.append(results)
            #
            stdout_file = op.join(self.log_path, '{}.out'.format(job_id))
            stderr_file = op.join(self.log_path, '{}.err'.format(job_id))
            # STDERR
            try:
                ifh = open(stderr_file + '.tmp', 'rt')
            except FileNotFoundError:
                try:
                    ifh = open(stderr_file, 'rt')
                except FileNotFoundError:
                    results['status'] = 'missing'
                    continue
            stderr_file_data = ifh.read().strip()
            ifh.close()
            if stderr_file_data.endswith('ERROR!'):
                results['status'] = 'error'
                continue
            elif stderr_file_data.endswith('DONE!'):
                results['status'] = 'done'
            else:
                results['status'] = 'frozen'
                continue
            # STDOUT
            ifh = open(stdout_file, 'r')
            try:
                results.update(json.load(ifh))
            except json.JSONDecodeError:
                # pass
                results['status'] = 'misformed output'
            finally:
                ifh.close()
        assert len(results_all) == len(iterable)
        results_df = pd.DataFrame(results_all)
        return results_df
