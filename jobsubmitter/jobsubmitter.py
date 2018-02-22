"""Main jobsubmitter code."""
import atexit
import concurrent.futures
import functools
import json
import logging
import os
import os.path as op
import subprocess
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

import attr
import pandas as pd
import paramiko
from tqdm import tqdm_notebook as tqdm

from .job_opts import JobOpts
from .system_command import get_system_command
from .utils import execute_remotely

logging.getLogger("paramiko").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

DATA_DIR = op.join(op.dirname(op.abspath(__file__)), 'data')
DEFAULT_CLUSTERS_FILE = op.join(DATA_DIR, 'clusters.yml')


class JobSubmitter:
    """.

    Notes:
        While it would be safer call the Python executable directly,
        this does not seem to be supported on PBS (at least not on banting :()).

    Args:

    """
    host: str
    concurrent_job_limit: int

    # Connection to the remote server (on the same cluster!)
    ssh: Optional[paramiko.SSHClient] = None

    def __init__(self, host: str, concurrent_job_limit: int = 0) -> None:
        """Initialize a JobSubmitter instance.

        Args:
            host: URL of the master node through which the jobs will be submitted.
            concurrent_job_limit: Maximum number of jubs that can be submitted to a cluster
                at a given time. ``0`` (default) means unlimited.
        """
        self.host = host
        self.host_opts = urlparse(host)
        self.concurrent_job_limit = concurrent_job_limit
        self.ssh = None

    @staticmethod
    def get_stdout_log(working_dir: Path, job_id: str, job_idx: int) -> Path:
        """Generate complete filename of the STDOUT log file."""
        return working_dir.joinpath(job_id).joinpath(f'{job_idx}.out')

    @staticmethod
    def get_stderr_log(working_dir: Path, job_id: str, job_idx: int) -> Path:
        """Generate complete filename of the STDERR log file."""
        return working_dir.joinpath(job_id).joinpath(f'{job_idx}.err')

    # #########################################################################
    # Manage connection
    # #########################################################################

    @contextmanager
    def connect(self):
        """Open connection to head node."""
        self._connect()
        yield
        self._disconnect()

    def _connect(self):
        if self.ssh is not None:
            logger.info("Already connected!")
            return
        self.ssh = paramiko.SSHClient()
        self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.ssh.connect(
            hostname=self.host_opts.hostname,
            port=self.host_opts.port,
            username=self.host_opts.username,
            password=self.host_opts.password)
        atexit.register(self._disconnect)

    def _disconnect(self):
        """Close connection to head node."""
        self.ssh.close()
        self.ssh = None
        atexit.unregister(self._disconnect)

    # #########################################################################
    # Submit jobs (requires connection with server)
    # #########################################################################

    def submit(self, df: pd.DataFrame, job_opts: JobOpts, deplay=0.02, progressbar=True):
        """Sumit jobs to the cluster.

        You have to establish a connection first (explicit is better than implicit)::

            with js.connect():
                js.submit([(0, 'echo "Hello world!"), (1, 'echo "Goodbye world!"')]
        """
        assert 'system_command' in df
        assert not df.duplicated().any()

        job_opts.working_dir.joinpath(job_opts.job_id).mkdir(parents=True, exist_ok=True)

        if self.host_opts.scheme in ['local']:
            worker = functools.partial(self._local_worker, job_opts=job_opts)
        else:
            worker = functools.partial(self._remote_worker, job_opts=job_opts)

        # Submit multiple jobs in parallel
        futures = []
        pool = concurrent.futures.ThreadPoolExecutor()
        for row in self._itertuples(df, progressbar=progressbar):
            future = pool.submit(worker, row)
            futures.append(future)
            time.sleep(deplay)
        pool.shutdown(wait=False)
        return futures

    def _itertuples(self, df, progressbar):
        for i, row in enumerate(
                tqdm(df.itertuples(), total=len(df), ncols=100, disable=not progressbar)):
            logger.debug("i: %s, row: %s", i, row)
            assert hasattr(row, 'Index') and hasattr(row, 'system_command')
            self._respect_concurrent_job_limit(i)
            yield row

    def _local_worker(self, row, job_opts) -> str:
        """

        TODO: This should return the id of the job running on the cluster.
        """
        stdout_log = self.get_stdout_log(job_opts.working_dir, job_opts.job_id, row.Index)
        stderr_log = self.get_stderr_log(job_opts.working_dir, job_opts.job_id, row.Index)
        with stdout_log.open('w') as stdout, stderr_log.open('w') as stderr:
            cp = subprocess.run(
                row.system_command,
                stdout=stdout,
                stderr=stderr,
                universal_newlines=True,
                shell=True)
            stderr.write('DONE!\n')
        return str(cp.returncode)

    def _remote_worker(self, row, job_opts) -> str:
        """

        TODO: This should return the id of the job running on the cluster.
        """
        env = {
            **job_opts.env,
            'SYSTEM_COMMAND': row.system_command,
            'STDOUT_LOG': self.get_stdout_log(job_opts.working_dir, job_opts.job_id, row.Index),
            'STDERR_LOG': self.get_stderr_log(job_opts.working_dir, job_opts.job_id, row.Index),
        }
        job_opts = attr.evolve(job_opts, env=env)
        system_command = get_system_command(self.host_opts.scheme, job_opts)
        stdout = execute_remotely(self.ssh, system_command)
        # A short break is required or else you can get weird errors:
        # ``Secsh channel 15 open FAILED: open failed: Administratively prohibited``
        time.sleep(0.02)
        return stdout

    def _respect_concurrent_job_limit(self, job_idx: int) -> None:
        """Limit the number of jobs running simultaneously."""
        STEP_SIZE = 50
        DELAY = 120
        if self.concurrent_job_limit and ((job_idx + 1) % STEP_SIZE) == 0:
            while (self.num_submitted_jobs + STEP_SIZE) > self.concurrent_job_limit:
                logger.info("'concurrent_job_limit' reached! Sleeping for {:.0f} minutes...".format(
                    DELAY / 60))
                time.sleep(DELAY)

    # #########################################################################
    # Monitor job status
    # #########################################################################

    def job_status(self, df: pd.DataFrame, job_opts: JobOpts, progressbar=True):
        """Read the status and results of each submitted job.

        Notes:
            - Multithrading does not make it faster :(.
        """
        # Refresh NFS:
        os.listdir(job_opts.working_dir.joinpath(job_opts.job_id))  # type: ignore
        results = [
            self._read_results(row, job_opts)
            for row in tqdm(df.itertuples(), total=len(df), ncols=100, disable=not progressbar)
        ]
        if not results:
            return pd.DataFrame(columns=['status', 'Index'])
        else:
            return pd.DataFrame(results).set_index('Index')

    def _read_results(self, row, job_opts):
        # Output files
        stdout_log = self.get_stdout_log(job_opts.working_dir, job_opts.job_id, row.Index)
        stderr_log = self.get_stderr_log(job_opts.working_dir, job_opts.job_id, row.Index)
        # === STDERR ===
        data = row._asdict()
        try:
            ifh = stderr_log.with_name(stderr_log.name + '.tmp').open('rt')
        except FileNotFoundError:
            try:
                ifh = stderr_log.open('rt')
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
        with stdout_log.open('r') as ifh:
            stdout_data = ifh.read().strip()
        try:
            data.update(json.loads(stdout_data))
        except (json.JSONDecodeError, TypeError):
            # `TypeError` in case JSON decodes something other than a dictionary
            data['stdout_data'] = stdout_data
        return data

    # #########################################################################
    # Cluster properties (requires connection with server)
    # #########################################################################

    @property
    def num_submitted_jobs(self) -> int:
        """Count the number of *submitted* jobs by the current user."""
        if self.host_opts.scheme == 'local':
            return None
        system_command = 'qstat -u "$USER" | grep "$USER" | wc -l'
        stdout = execute_remotely(self.ssh, system_command)
        try:
            num_submitted_jobs = int(stdout)
        except ValueError:
            num_submitted_jobs = 999999
        return num_submitted_jobs

    @property
    def num_running_jobs(self) -> int:
        """Count the number of *running* jobs by the current user."""
        if self.host_opts.scheme == 'local':
            return None
        system_command = 'qstat -u "$USER" | grep "$USER" | grep -i " r  " | wc -l'
        stdout = execute_remotely(self.ssh, system_command)
        logger.debug(stdout)
        num_running_jobs = int(stdout)
        return num_running_jobs
