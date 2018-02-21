import os.path as op
from pathlib import Path
from typing import Dict, NamedTuple, Optional

import attr

import jobsubmitter
from jobsubmitter.utils import execute_remotely


@attr.s(auto_attribs=True, slots=True)
class JobOpts:
    """Collection of obtions for one or more jobs.

    Attributes:
        job_name: Name of the job.
        queue: The name of the queue to which the job should be submitted.
        nproc: Number of processors per node.
        walltime: Maximum amount of time that the job is allowed to run, written as `hh:mm:ss`.
        mem: Maximum RAM per node.
        pmem: Maximum RAM per process.
        vmem: Virtual memory per node (not enforced).
        pvmem: Virtual memory per processor (not enforced).
        email: Email to which job status updateds should be send
            (prepare for a potentially-large volume of emails!).
        qsub_shell: The user login shell. This is typically ``/bin/bash``,
            since, for example, the ``python`` shell does not work on banting :(.
        qsub_script: Bash script that is used to submit jobs.
            You should generally stick with 
        array_jobs: Specified using the PBS / Slurm convention.
            (For example, '1-100%1' means run 100 jobs with only one job running at a time).
    """
    job_name: str = attr.ib()
    # Resources
    nproc: int = 1
    walltime: str = '02:00:00'
    mem: str = '0'
    pmem: Optional[str] = None
    vmem: Optional[str] = None
    pvmem: Optional[str] = None
    gpus: Optional[int] = None
    array_jobs: Optional[str] = None
    working_dir: Optional[str] = None
    # Allocation
    account: Optional[str] = None
    queue: Optional[str] = None
    email: Optional[str] = None
    # Environment
    env: Optional[Dict[str, str]] = None
    qsub_shell: str = '/bin/bash'
    qsub_script: Path = Path(jobsubmitter.scripts.__path__[0]).joinpath('qsub.sh')
    """Bash script which is used to submit jobs."""

    @array_jobs.validator
    def check(self, attribute, value):
        if value > 42:
            raise ValueError("x must be smaller or equal to 42")
