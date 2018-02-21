from pathlib import Path
from typing import Dict, Optional

import attr

import jobsubmitter


@attr.s(auto_attribs=True, slots=True)
class JobOpts:
    """Collection of obtions for one or more jobs.

    Attributes:
        job_id: Name of the job.
        working_dir:
        nproc: Number of processors per node.
        walltime: Maximum amount of time that the job is allowed to run, written as `hh:mm:ss`.
        mem: Maximum RAM per node.
        pmem: Maximum RAM per process.
        vmem: Virtual memory per node (not enforced).
        pvmem: Virtual memory per processor (not enforced).
        gpus: ...
        array_jobs: Specified using the PBS / Slurm convention.
            For example, ``1-100%1`` means run 100 jobs with only one job running at a time.
        account:
        queue:
        email: Email to which job status updateds should be send.
            **Do not provide your work email**, as the cluster could potentially send many
            thousdands of emails, DDOSing your email account.
        env: Environment variables to supply to the job.
        qsub_shell: The user login shell.
            This is typically ``/bin/bash``, since, for example, the ``python`` shell
            does not work on PBS :(.
        qsub_script: Bash script that is used to submit jobs.
            The bash script that comes with `jobsubmitter` should be sufficient in most cases.
    """
    job_id: str = attr.ib()
    # Resources
    nproc: int = 1
    walltime: str = '02:00:00'
    mem: str = '0'
    pmem: Optional[str] = None
    vmem: Optional[str] = None
    pvmem: Optional[str] = None
    gpus: Optional[int] = None
    array_jobs: Optional[str] = None
    working_dir: Path = attr.ib(default=Path.cwd(), validator=attr.validators.instance_of(Path))
    # Allocation
    account: Optional[str] = None
    queue: Optional[str] = None
    email: Optional[str] = None
    # Environment
    env: Optional[Dict[str, str]] = None
    qsub_shell: str = '/bin/bash'
    qsub_script: Path = attr.ib(
        default=(
            Path(jobsubmitter.__path__[0])  # type: ignore
            .joinpath('scripts').joinpath('qsub.sh')),
        validator=attr.validators.instance_of(Path))
