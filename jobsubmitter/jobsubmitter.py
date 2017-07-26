"""


To Do
-----

- $HOME
- $SCRATCH

"""
import atexit
import concurrent.futures
import contextlib
import json
import logging
import os
import os.path as op
import shlex
import subprocess
import time
from collections import namedtuple

import pandas as pd
import paramiko
from retrying import retry

from kmtools.db_tools import parse_connection_string

logging.getLogger("paramiko").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

#: Connection options
#: If `remote_home` and `remote_scratch` are `None`, it implies
#: that they are the same as $HOME on local host.
_ConOpts = namedtuple('ConOpts', 'connection_string, remote_home, remote_scratch')
_ConOpts.__new__.__defaults__ = (None, None, )

KNOWN_HOSTS = {
    'local': _ConOpts('local://:@127.0.0.1'),
    'beagle': _ConOpts('sge://:@192.168.6.201'),
    'banting': _ConOpts('pbs://:@192.168.233.150'),
    'scinet-gpc': _ConOpts('pbs://:@login.scinet.utoronto.ca-gpc02', '$HOME', '$SCRATCH'),
    'cq-ms2': _ConOpts('pbs://:@pmkim-ms.ccs.usherbrooke.ca', '$HOME', '$HOME'),
    'cq-mp2': _ConOpts('pbs://:@pmkim-mp.ccs.usherbrooke.ca', '$HOME', '$HOME'),
    'cq-guillimin': _ConOpts('pbs://:@guillimin.hpc.mcgill.ca', '$HOME', '$SCRATCH'),
}


class QSub:

    def __init__(
        self,
        job_type,
        job_name,
        qsub_script,
        working_dir,
        qsub_script_args=None,  # key-value dict
        queue='medium',
        nproc=1,  # number of processors per node
        walltime='02:00:00',  # hh:mm:ss
        mem=None,  # RAM per node
        pmem=None,  # RAM per processor
        vmem=None,  # virtual memory per node (not enforced)
        pvmem=None,  # virtual memory per processor (not enforced)
        email='noname@example.com',  # email of user submitting jobs
        email_opts='a',  # email options
        qsub_shell='/bin/bash',  # python does not work on banting :(
        env=None,
    ):
        """Class for generating ``qsub`` system commands.

        Parameters
        ----------
        job_type : {'sge', 'pbs'}
            Specifies which type of batch system you are using.
        job_name : str
            Name of the job.
        qsub_script : str
            Location of script file to be ran by qsub.
        working_dir : str
            Location relative to which all system commands are performed.
        queue : str, optional
        nproc : int, optional
            Number of processors per node.
        walltime : str, optional
            Maximum amount of time that the job is allowed to run,
            written as `hh:mm:ss`.
        mem : str, optional
            Maximum amount of physical memory used by the job.
        pmem : str, optional
            Maximum amount of physical memory used by any single process of the job.
        vmem : str, optional
            Maximum amount of virtual memory used by all (conc) processes in the job.
        pvmem : str, optional
            Maximum amount of virtual memory used by any single process in the job.
        email : str, optional
            Email of user submitting jobs
        email_opts : str, optional
            Email options
        qsub_shell : str, optional
            The ``python`` shell does not work on banting :(.
        qsub_script_args : dict, optional
            Arguments for the ``qsub.sh`` shell script.
        env : dict, optional
            Environment variables.
        """
        self.job_type = job_type
        self.job_name = job_name
        self.qsub_script = qsub_script
        self.working_dir = working_dir
        self.queue = queue
        self.nproc = nproc
        self.walltime = walltime
        self.mem = mem
        self.pmem = pmem
        self.vmem = vmem
        self.pvmem = pvmem
        self.email = email
        self.email_opts = email_opts
        self.qsub_shell = qsub_shell
        self.qsub_script_args = qsub_script_args or {}
        self.env = env or {}

        self._qsub_system_command = (
            self._format_local(self._format_global(self._qsub_system_command_template)))

    @property
    def _qsub_system_command_template(self):
        """Template of the system command which will submit the given job on the head node.

        - Global options are inside one set of curly braces.
        - Local options are inside two sets of curly braces.
        """
        return """\
PATH="{{PATH}}" qsub -S {qsub_shell} -N {job_name} -M {email} -m {email_opts} \
-o /dev/null -e /dev/null {{working_dir}} \
{{nproc}}{{walltime}}{{mem}}{{pmem}}{{vmem}}{{pvmem}} {{{{env_string}}}} \
'{qsub_script}' {{qsub_script_args_string}} \
"""

    def _format_global(self, template):
        return template.format(**self.__dict__)

    def _format_local(self, template):
        cluster_opts = dict(
            PATH=self.env.get('PATH', '$PATH'),
        )
        if self.job_type == 'sge':
            cluster_opts = dict(
                **cluster_opts,
                working_dir=' -wd {}'.format(self.working_dir),
                nproc=' -pe smp {}'.format(self.nproc),
                walltime=' -l h_rt={}'.format(self.walltime),
                mem=' -l mem_free={}'.format(self.mem) if self.mem else '',
                pmem='',
                vmem=' -l h_vmem={}'.format(self.vmem) if self.vmem else '',
                pvmem='',
                qsub_script_args_string=' '.join(
                    '--{} {}'.format(key, value) for key, value in self.qsub_script_args.items())
                if self.qsub_script_args else '',
            )
        elif self.job_type == 'pbs':
            cluster_opts = dict(
                **cluster_opts,
                working_dir=' -d {}'.format(self.working_dir),
                nproc='-l nodes=1:ppn={}'.format(self.nproc) if self.nproc else '-l nodes=1',
                walltime=',walltime={}'.format(self.walltime),
                mem=',mem={}'.format(self.mem) if self.mem else '',
                pmem=',pmem={}'.format(self.pmem) if self.pmem else '',
                vmem=',vmem={}'.format(self.vmem) if self.vmem else '',
                pvmem=',pvmem={}'.format(self.pvmem) if self.pvmem else '',
                qsub_script_args_string='-F "{}"'.format(
                    ' '.join(
                        '--{} {}'.format(key, value)
                        for key, value in self.qsub_script_args.items()))
                if self.qsub_script_args else '',
            )
        else:
            raise Exception
        return template.format(**cluster_opts)

    def format_env(self, env):
        if self.job_type == 'sge':
            env_string = ' '.join('-v {}="{}"'.format(*x) for x in env.items())
        elif self.job_type == 'pbs':
            env_string = '-v ' + ','.join('{}="{}"'.format(*x) for x in env.items())
        return env_string

    def generate_qsub_system_command(self, system_command, stdout_log, stderr_log):
        if self.job_type == 'pbs' and ',' in system_command:
            raise Exception(
                "Can't have commas in 'system_command' when using 'pbs'.\n{}"
                .format(system_command))
        return self._qsub_system_command.format(
            env_string=self.format_env({
                **self.env,
                'SYSTEM_COMMAND': system_command,
                'STDOUT_LOG': stdout_log,
                'STDERR_LOG': stderr_log
            }))


class JobSubmitter:
    """.

    .. note::

        While it would be safer call the Python executable directly,
        this does not seem to be supported on PBS (at least not on banting :()).

    Attributes
    ----------

    head_node_type
    head_node_ip

    """

    def __init__(
            self,
            job_folder,  # can be relative to $HOME
            connection_string=None,
            *,
            data_folder=None,  # can be relative to $HOME
            remote_home=None,
            remote_scratch=None,
            concurrent_job_limit=None,
            **kwargs):
        """.

        Parameters
        ----------
        kwargs : dict, optional
            Arguments to be passed to :class:`.QSub`.

        To do
        -----
            - Add more options to the connecton string (username / pass).
        """
        # Job info
        self.job_name = op.basename(job_folder)
        self.job_abspath = (
            job_folder if op.isabs(job_folder) else op.join(op.expanduser('~'), job_folder))
        self.job_relpath = op.relpath(self.job_abspath, op.expanduser('~'))

        if data_folder is None:
            self.data_abspath = self.job_abspath
        elif not op.isabs(data_folder):
            self.data_abspath = op.join(op.expanduser('~'), data_folder)
        else:
            self.data_abspath = data_folder
        self.data_relpath = op.relpath(self.data_abspath, op.expanduser('~'))

        # Connection string
        if connection_string in KNOWN_HOSTS:
            if remote_home is None:
                remote_home = KNOWN_HOSTS[connection_string].remote_home
            if remote_scratch is None:
                remote_scratch = KNOWN_HOSTS[connection_string].remote_scratch
            connection_string = KNOWN_HOSTS[connection_string].connection_string

        head_node_type = None
        head_node_ip = None
        if connection_string:
            _db_info = parse_connection_string(connection_string)
            logger.debug("_db_info: %s", _db_info)
            head_node_type = _db_info['db_type']
            head_node_ip = _db_info['db_url']

        if head_node_type not in ['local', 'sge', 'pbs']:
            raise ValueError("Wrong 'head_node_type': '{}'".format(head_node_type))

        self.head_node_type = head_node_type
        self.head_node_ip = head_node_ip
        self._ssh = None
        self._connect()
        assert self._ssh is not None
        self.env = kwargs.get('env', {})

        # Remote paths
        assert self._ssh is not None
        self.use_remote = remote_home is not None
        if self.use_remote:
            self.remote_home = self._format_remote_path(remote_home)
            self.remote_scratch = self._format_remote_path(remote_scratch)
            self.remote_job_abspath = op.join(self.remote_scratch, self.job_relpath)
            self.remote_data_abspath = op.join(self.remote_scratch, self.data_relpath)
        else:
            self.remote_home = None
            self.remote_scratch = None
            self.remote_job_abspath = None
            self.remote_data_abspath = None

        # Make a job directory
        assert self._ssh is not None
        logger.debug("Use remote: %s", self.use_remote)
        if self.use_remote:
            system_command = 'ssh {head_node_ip} "mkdir -p {remote_job_abspath}"'.format(
                head_node_ip=self.head_node_ip, remote_job_abspath=self.remote_job_abspath)
            logger.debug(system_command)
            cp = subprocess.run(shlex.split(system_command))
            cp.check_returncode()
        else:
            try:
                os.mkdir(self.job_abspath)
            except FileExistsError:
                logger.warning("Using an existing folder for log output. This is dangerous!!!")
        time.sleep(1)  # give some time to sync NFS

        # QSub
        assert self._ssh is not None
        self.concurrent_job_limit = concurrent_job_limit
        if self.head_node_type != 'local':
            qsub_script = self._get_qsub_script()
            working_dir = self._get_working_dir()
            self.qsub = QSub(
                self.head_node_type, self.job_name, qsub_script, working_dir, **kwargs)

    def _format_remote_path(self, remote_path):
        assert remote_path is not None
        if remote_path.startswith('$'):
            remote_path, *_ = self._exec_system_command('echo "{}"'.format(remote_path))
        return remote_path

    def _get_qsub_script(self):
        if 'PATH' in self.env:
            PATH = self.env['PATH']
        else:
            PATH = '$HOME/anaconda/bin:$PATH'
        system_command = 'export PATH="{}"; which qsub.sh'.format(PATH)
        qsub_script, *_ = self._exec_system_command(system_command)
        return qsub_script

    def _get_working_dir(self):
        if self.use_remote:
            return self.remote_data_abspath
        else:
            return self.data_abspath

    # === Manage connection ===

    @contextlib.contextmanager
    def connect(self):
        """Open connection to head node."""
        self._connect()
        yield
        self._disconnect()

    def _connect(self):
        if self._ssh is not None:
            logger.info("Already connected!")
            return
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(self.head_node_ip)
        self._ssh = ssh
        atexit.register(self._disconnect)

    def _disconnect(self):
        """Close connection to head node."""
        self._ssh.close()
        self._ssh = None
        atexit.unregister(self._disconnect)

    # === Submit job (requires connection with server) ===

    def submit(self, iterable):
        """Sumit jobs to the cluster.

        You have to establish a connection first (explicit is better than implicit)::

            with js.connect():
                js.submit([(0, 'echo "Hello world!"), (1, 'echo "Goodbye world!"')]
        """
        if self._ssh is None:
            self._connect()
        p = concurrent.futures.ThreadPoolExecutor()
        if self.head_node_type == 'local':
            worker = self._local_worker
        else:
            worker = self._remote_worker
        futures = []
        for i, (job_id, job_data) in enumerate(iterable):
            logger.debug("(i, job_id, job_data): (%s, %s, %s)", i, job_id, job_data)
            assert 'system_command' in job_data
            self._respect_concurrent_job_limit(i)
            futures.append(p.submit(worker, i, job_id, dict(job_data)))
            # A short break is required or else you can get weird errors:
            # "Secsh channel 15 open FAILED: open failed: Administratively prohibited"
            time.sleep(0.05)
        p.shutdown(wait=False)
        return futures

    def _local_worker(self, i, job_id, job_data):
        system_command = job_data['system_command']
        with open(self.stdout_log.format(job_id), 'w') as stdout, \
                open(self.stderr_log.format(job_id), 'w') as stderr:
            cp = subprocess.run(
                system_command, stdout=stdout, stderr=stderr, universal_newlines=True, shell=True)
            stderr.write('DONE!\n')
        return cp.returncode

    def _remote_worker(self, i, job_id, job_data):
        system_command = job_data['system_command']
        qsub_system_command = self.qsub.generate_qsub_system_command(
            system_command, self.stdout_log.format(job_id), self.stderr_log.format(job_id))
        stdout, *_ = self._exec_system_command(qsub_system_command)
        time.sleep(0.02)
        return stdout

    @property
    def stdout_log(self):
        if self.head_node_type == 'local':
            stdout_log = op.join(self.job_abspath, '{}.out')
        else:
            stdout_log = op.join(
                self.job_abspath if not self.use_remote else self.remote_job_abspath, '{}.out')
        return stdout_log

    @property
    def stderr_log(self):
        if self.head_node_type == 'local':
            stderr_log = op.join(self.job_abspath, '{}.err')
        else:
            stderr_log = op.join(
                self.job_abspath if not self.use_remote else self.remote_job_abspath, '{}.err')
        return stderr_log

    @property
    def num_submitted_jobs(self):
        if self.head_node_type == 'local':
            return None
        system_command = 'qstat -u "$USER" | grep "$USER" | wc -l'
        stdout, *_ = self._exec_system_command(system_command)
        num_submitted_jobs = int(stdout)
        return num_submitted_jobs

    @property
    def num_running_jobs(self):
        if self.head_node_type == 'local':
            return None
        system_command = 'qstat -u "$USER" | grep "$USER" | grep -i " r  " | wc -l'
        stdout, *_ = self._exec_system_command(system_command)
        num_running_jobs = int(stdout)
        return num_running_jobs

    def _exec_system_command(self, system_command):
        assert self._ssh is not None
        n_tries = 0
        stdout = '...'
        stderr = '...'
        logger.debug(system_command)
        while n_tries < 5 and stderr:
            if n_tries:
                delay = (1 * n_tries)
                logger.debug('Sleeping for {} seconds...'.format(delay))
                time.sleep(delay)
            n_tries += 1
            stdin_fh, stdout_fh, stderr_fh = self._ssh.exec_command(
                system_command, get_pty=True, environment=self.env)
            stdout = stdout_fh.read().decode().strip()
            if stdout:
                logger.info(stdout)
            stderr = stderr_fh.read().decode().strip()
            if stderr:
                logger.warning(stderr)
        return stdout, stderr

    def _respect_concurrent_job_limit(self, i):
        # Limit the number of jobs running simultaneously
        STEP_SIZE = 50
        DELAY = 120
        if self.concurrent_job_limit is not None and ((i + 1) % STEP_SIZE) == 0:
            while (self.num_submitted_jobs + STEP_SIZE) > self.concurrent_job_limit:
                logger.info(
                    "'concurrent_job_limit' reached! Sleeping for {:.0f} minutes..."
                    .format(DELAY / 60))
                time.sleep(DELAY)

    # === Sync ===

    @retry(
        retry_on_exception=lambda exc: isinstance(exc, subprocess.SubprocessError),
        stop_max_attempt_number=3)
    def _sync_remote(self, local_abspath, remote_abspath):
        system_command = """\
rsync -az --update --exclude '*.tmp' {local_abspath}/ {head_node_ip}:{remote_abspath}/ \
""".format(
            local_abspath=local_abspath, head_node_ip=self.head_node_ip,
            remote_abspath=remote_abspath)
        logger.debug(system_command)
        cp = subprocess.run(
            shlex.split(system_command), stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            universal_newlines=True)
        cp.check_returncode()

    @retry(
        retry_on_exception=lambda exc: isinstance(exc, subprocess.SubprocessError),
        stop_max_attempt_number=3)
    def _sync_local(self, local_abspath, remote_abspath):
        system_command = """\
rsync -az --update --exclude '*.tmp' {head_node_ip}:{remote_abspath}/ {local_abspath}/ \
""".format(
            local_abspath=local_abspath, head_node_ip=self.head_node_ip,
            remote_abspath=remote_abspath)
        logger.debug(system_command)
        cp = subprocess.run(
            shlex.split(system_command), stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            universal_newlines=True)
        cp.check_returncode()

    def sync_remote(self):
        self._sync_remote(self.job_abspath, self.remote_job_abspath)

    def sync_local(self):
        self._sync_local(self.job_abspath, self.remote_job_abspath)

    def sync_data(self):
        if self.data_abspath is None or self.remote_data_abspath is None:
            logger.warning(
                "Data folders are not set! data_abspath: '{}', remote_data_abspath: '{}'"
                .format(self.data_abspath, self.remote_data_abspath))
        self._sync_remote(self.data_abspath, self.remote_data_abspath)
        self._sync_local(self.data_abspath, self.remote_data_abspath)

    # === Job status ===

    def job_status(self, iterable):
        """Return a :class:`pandas.DataFrame` with the status and results of each submitted job.

        Parameters
        ----------
        iterable :
            An iterable of ``(job_id, system_command)`` tuples.
            ``system_command`` can be None, in which case the ``system_command`` column
            of the returned DataFrame will be empty.
        """
        # Refresh NFS
        os.listdir(op.join(self.job_abspath))
        results = []
        for i, (job_id, job_data) in enumerate(iterable):
            row = {'job_id': job_id, **job_data}
            results.append(row)
            # Output files
            stdout_log = self.stdout_log.format(job_id)
            stderr_log = self.stderr_log.format(job_id)
            # === STDERR ===
            try:
                ifh = open(stderr_log + '.tmp', 'rt')
            except FileNotFoundError:
                try:
                    ifh = open(stderr_log, 'rt')
                except FileNotFoundError:
                    row['status'] = 'missing'
                    continue
            stderr_file_data = ifh.read().strip().lower()
            ifh.close()
            if stderr_file_data.endswith('error!'):
                row['status'] = 'error'
                continue
            elif stderr_file_data.endswith('done!'):
                row['status'] = 'done'
            else:
                row['status'] = 'frozen'
                continue
            # === STDOUT ===
            with open(stdout_log, 'r') as ifh:
                stdout_data = ifh.read().strip()
            try:
                row.update(json.loads(stdout_data))
            except json.JSONDecodeError:
                row['stdout_data'] = stdout_data
        assert len(results) == (i + 1)
        results_df = pd.DataFrame(results).set_index('job_id')
        return results_df
