import atexit
import concurrent.futures
import contextlib
import functools
import json
import logging
import os
import os.path as op
import shlex
import subprocess
import time
from textwrap import dedent
from typing import Any, Dict, Optional, Union

import pandas as pd
import paramiko
from tqdm import tqdm

from kmtools import system_tools
from kmtools.db_tools import ConOpts, parse_connection_string

from .cluster_opts import ClusterOpts
from .job_opts import JobOpts, PBSSystemCommand, SGESystemCommand, SLURMSystemCommand, SystemCommand
from .utils import execute_remotely

logging.getLogger("paramiko").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)


class JobSubmitter:
    """.

    Notes:
        While it would be safer call the Python executable directly,
        this does not seem to be supported on PBS (at least not on banting :()).

    """
    job_name: str
    job_abspath: str
    data_abspath: str
    env: Optional[Dict[str, str]] = None
    # Named tuples
    cluster_opts: ClusterOpts
    con_opts: ConOpts
    # Connection to the remote server
    ssh: Optional[paramiko.SSHClient] = None
    # Paths on the remote server
    remote_job_abspath: Optional[str] = None
    remote_data_abspath: Optional[str] = None
    # Cache
    _host_ip: Optional[str] = None
    _qsub_script: Optional[str] = None

    def __init__(self,
                 cluster: Union[str, ClusterOpts],
                 job_folder: str,
                 data_folder: Optional[str] = None,
                 env: Optional[Dict[str, str]] = None) -> None:
        """Initialize a JobSubmitter instance.

        Args:
            cluster: Name of the cluster to which the jobs will be submitted.
            job_folder: Local location of the job log files.
                This can be relative to ``$HOME``.
            data_folder: Local location of data files.
                If provided, this will become the working folder of the job.
                This can be relative to ``$HOME``.
        """
        self.job_name = op.basename(job_folder)
        self.job_abspath = op.expanduser(job_folder)
        self.data_abspath = op.expanduser(data_folder) if data_folder else self.job_abspath
        self.env = env

        if isinstance(cluster, ClusterOpts):
            self.cluster_opts = cluster
        else:
            self.cluster_opts = ClusterOpts._from_file(cluster)

        self.con_opts = parse_connection_string(self.cluster_opts.connection_string)
        if self.con_opts.name not in ['local', 'sge', 'pbs', 'slurm']:
            raise ValueError("Wrong head node type: '{}'".format(self.con_opts.name))

        self._connect()
        self._set_remote_paths()
        self._create_job_dir()

    @property
    def host_ip(self) -> str:
        """Return the IP address of the host.

        The host is the computer submitting requests to the queue.
        (Typically, it is the computer running the Jupyter notebook).
        """
        if self._host_ip:
            return self._host_ip
        self._host_ip, _ = self.ssh.get_transport().sock.getsockname()
        return self._host_ip

    @property
    def qsub_script(self) -> str:
        if self._qsub_script:
            return self._qsub_script
        PATH = self.env.get('PATH', '$PATH') if self.env is not None else '$PATH'
        system_command = f"""export PATH="{PATH}"; which qsub.sh"""
        self._qsub_script = execute_remotely(system_command, self.ssh)
        return self._qsub_script

    @property
    def job_working_dir(self) -> str:
        return self.remote_data_abspath if self.remote_data_abspath else self.data_abspath

    def get_stdout_log(self, job_id: int) -> str:
        """Generate complete filename of the STDOUT log file."""
        if self.con_opts.name == 'local' or not self.remote_job_abspath:
            job_abspath = self.job_abspath
        else:
            job_abspath = self.remote_job_abspath
        stdout_log = op.join(job_abspath, f'{job_id}.out')
        return stdout_log

    def get_stderr_log(self, job_id: int) -> str:
        """Generate complete filename of the STDERR log file."""
        if self.con_opts.name == 'local' or not self.remote_job_abspath:
            job_abspath = self.job_abspath
        else:
            job_abspath = self.remote_job_abspath
        stderr_log = op.join(job_abspath, f'{job_id}.err')
        return stderr_log

    # #########################################################################

    def _set_remote_paths(self) -> None:
        if self.cluster_opts.remote_home:
            assert self.con_opts.remote_scratch is not None
            job_relpath = op.relpath(self.job_abspath, op.expanduser('~'))
            data_relpath = op.relpath(self.data_abspath, op.expanduser('~'))
            if self.cluster_opts.remote_home.startswith('$'):
                self.con_opts = self.con_opts._replace(
                    remote_home=self._format_remote_path(self.cluster_opts.remote_home))
            if self.con_opts.remote_scratch.startswith('$'):
                self.con_opts = self.con_opts._replace(
                    remote_scratch=self._format_remote_path(self.con_opts.remote_scratch))
            self.remote_job_abspath = op.join(self.con_opts.remote_scratch, job_relpath)
            self.remote_data_abspath = op.join(self.con_opts.remote_scratch, data_relpath)

    def _create_job_dir(self) -> None:
        """Create a folder for storing job logs."""
        if self.cluster_opts.remote_home:
            system_command = f'ssh {self.con_opts.url} "mkdir -p {self.remote_job_abspath}"'
            logger.debug(system_command)
            subprocess.check_call(shlex.split(system_command))
        else:
            os.makedirs(self.job_abspath, exist_ok=True)
        time.sleep(1)  # Give some time to sync NFS

    def _format_remote_path(self, remote_path):
        if remote_path.startswith('$'):
            remote_path = execute_remotely('echo "{}"'.format(remote_path), self.ssh)
        return remote_path

    # #########################################################################
    # Manage connection
    # #########################################################################

    @contextlib.contextmanager
    def connect(self):
        """Open connection to head node."""
        self._connect()
        yield
        self._disconnect()

    def _connect(self):
        if self.ssh is not None:
            logger.info("Already connected!")
            return
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(self.con_opts.url)
        self.ssh = ssh
        atexit.register(self._disconnect)

    def _disconnect(self):
        """Close connection to head node."""
        self.ssh.close()
        self.ssh = None
        atexit.unregister(self._disconnect)

    # #########################################################################
    # Submit jobs (requires connection with server)
    # #########################################################################

    def _itertuples(self, df):
        for i, row in enumerate(tqdm(df.itertuples(), total=len(df), ncols=100)):
            logger.debug("i: %s, row: %s", i, row)
            assert hasattr(row, 'Index') and hasattr(row, 'system_command')
            self._respect_concurrent_job_limit(i)
            yield row

    def submit(self, df: pd.DataFrame, job_opts: JobOpts):
        """Sumit jobs to the cluster.

        You have to establish a connection first (explicit is better than implicit)::

            with js.connect():
                js.submit([(0, 'echo "Hello world!"), (1, 'echo "Goodbye world!"')]
        """
        assert 'system_command' in df
        assert len(df) == len(df.index.drop_duplicates())

        worker: Any
        tq: SystemCommand
        if self.con_opts.name == 'local':
            worker = self._local_worker
        elif self.con_opts.name == 'sge':
            tq = SGESystemCommand(self.qsub_script, self.job_working_dir, job_opts)
            worker = functools.partial(self._remote_worker, tq=tq)
        elif self.con_opts.name == 'pbs':
            tq = PBSSystemCommand(self.qsub_script, self.job_working_dir, job_opts)
            worker = functools.partial(self._remote_worker, tq=tq)  # type: ignore
        elif self.con_opts.name == 'slurm':
            tq = SLURMSystemCommand(self.qsub_script, self.job_working_dir, job_opts)
            worker = functools.partial(self._remote_worker, tq=tq)  # type: ignore
        else:
            raise ValueError(f"Wrong head node type: '{self.con_opts.name}'")

        # Submit multiple jobs in parallel
        futures = []
        pool = concurrent.futures.ThreadPoolExecutor()
        for row in self._itertuples(df):
            future = pool.submit(worker, row)
            futures.append(future)
            time.sleep(0.02)
        pool.shutdown(wait=False)
        return futures

    def _local_worker(self, row):
        stdout_log = self.get_stdout_log(row.Index)
        stderr_log = self.get_stderr_log(row.Index)
        with open(stdout_log, 'w') as stdout, open(stderr_log, 'w') as stderr:
            cp = subprocess.run(
                row.system_command,
                stdout=stdout,
                stderr=stderr,
                universal_newlines=True,
                shell=True)
            stderr.write('DONE!\n')
        return str(cp.returncode)

    def _remote_worker(self, row, tq: SystemCommand):
        env = {
            **self.env,
            'SYSTEM_COMMAND': row.system_command,
            'STDOUT_LOG': self.get_stdout_log(row.Index),
            'STDERR_LOG': self.get_stderr_log(row.Index),
            'HOST_IP': self.host_ip,
        }
        system_command = tq.get_system_command(env)
        stdout = execute_remotely(system_command, self.ssh)
        # A short break is required or else you can get weird errors:
        # "Secsh channel 15 open FAILED: open failed: Administratively prohibited"
        time.sleep(0.02)
        return stdout

    @property
    def num_submitted_jobs(self) -> int:
        """Count the number of *submitted* jobs by the current user."""
        if self.con_opts.name == 'local':
            return None
        system_command = 'qstat -u "$USER" | grep "$USER" | wc -l'
        stdout = execute_remotely(system_command, self.ssh)
        try:
            num_submitted_jobs = int(stdout)
        except ValueError:
            num_submitted_jobs = 999999
        return num_submitted_jobs

    @property
    def num_running_jobs(self) -> int:
        """Count the number of *running* jobs by the current user."""
        if self.con_opts.name == 'local':
            return None
        system_command = 'qstat -u "$USER" | grep "$USER" | grep -i " r  " | wc -l'
        stdout = execute_remotely(system_command, self.ssh)
        logger.debug(stdout)
        num_running_jobs = int(stdout)
        return num_running_jobs

    def _respect_concurrent_job_limit(self, job_idx: int) -> None:
        """Limit the number of jobs running simultaneously."""
        STEP_SIZE = 50
        DELAY = 120
        if self.cluster_opts.concurrent_job_limit is not None and ((job_idx + 1) % STEP_SIZE) == 0:
            while (self.num_submitted_jobs + STEP_SIZE) > self.cluster_opts.concurrent_job_limit:
                logger.info("'concurrent_job_limit' reached! Sleeping for {:.0f} minutes...".format(
                    DELAY / 60))
                time.sleep(DELAY)

    # #########################################################################
    # Monitor job status
    # #########################################################################

    def job_status(self, df: pd.DataFrame):
        """Read the status and results of each submitted job.

        Notes:
            - Multithrading does not make it faster :(.
        """
        os.listdir(op.join(self.job_abspath))  # refresh NFS
        results = [self._read_results(row) for row in self._itertuples(df)]
        results_df = pd.DataFrame(results).set_index('Index')
        return results_df

    def _read_results(self, row):
        # Output files
        stdout_log = self.get_stdout_log(row.Index)
        stderr_log = self.get_stderr_log(row.Index)
        # === STDERR ===
        data = row._asdict()
        try:
            ifh = open(stderr_log + '.tmp', 'rt')
        except FileNotFoundError:
            try:
                ifh = open(stderr_log, 'rt')
            except FileNotFoundError:
                data['status'] = 'missing'
                return data
        stderr_file_data = ifh.read().strip().lower()
        ifh.close()
        if stderr_file_data.endswith('error!'):
            data['status'] = 'error'
            return data
        elif stderr_file_data.endswith('done!'):
            data['status'] = 'done'
        else:
            data['status'] = 'frozen'
            return data
        # === STDOUT ===
        with open(stdout_log, 'r') as ifh:
            stdout_data = ifh.read().strip()
        try:
            data.update(json.loads(stdout_data))
        except json.JSONDecodeError:
            data['stdout_data'] = stdout_data
        return data

    # #########################################################################
    # Sync remote and local folders
    # #########################################################################

    @system_tools.retry_subprocess
    def sync_logs(self):
        """Copy job logs from the remote job folder to the local job folder."""
        system_command = dedent(f"""\
            rsync -az --update --delete --exclude '*.tmp'
            {self.con_opts.url}:{self.remote_job_abspath}/
            {self.job_abspath}/
            """).replace('\n', ' ')
        system_tools.execute(system_command)

    @system_tools.retry_subprocess
    def sync_data(self):
        """Bring the remote and local data folders in sync."""
        if self.data_abspath is None or self.remote_data_abspath is None:
            logger.warning("Data folders are not set! "
                           "data_abspath: '%s', remote_data_abspath: '%s'", self.data_abspath,
                           self.remote_data_abspath)

        system_command = dedent(f"""\
            rsync -az --update --exclude '*.tmp'
            {self.data_abspath}/
            {self.con_opts.url}:{self.remote_data_abspath}/
            """).replace('\n', ' ')
        system_tools.execute(system_command)

        system_command = dedent(f"""\
            rsync -az --update --exclude '*.tmp'
            {self.con_opts.url}:{self.remote_data_abspath}/
            {self.data_abspath}/
            """).replace('\n', ' ')
        system_tools.execute(system_command)
