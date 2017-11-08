from textwrap import dedent
from typing import Dict, NamedTuple, Optional


class JobOpts(NamedTuple):
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
        email_opts: Email options.
        qsub_shell: The user login shell. This is typically ``/bin/bash``,
            since, for example, the ``python`` shell does not work on banting :(.
        qsub_script_args: Arguments for the ``qsub.sh`` shell script.
        env: Environment variables that should be passed to the job.
    """
    job_name: str
    queue: str = 'medium'
    nproc: int = 1
    walltime: str = '02:00:00'
    mem: Optional[str] = None
    pmem: Optional[str] = None
    vmem: Optional[str] = None
    pvmem: Optional[str] = None
    email: Optional[str] = 'noname@example.com'
    email_opts: str = 'a'
    qsub_shell: str = '/bin/bash'
    qsub_script_args: Optional[Dict[str, str]] = None


class SystemCommand:
    qsub_script: str
    working_dir: str
    jo: JobOpts

    def __init__(self, qsub_script: str, working_dir: str, jo: JobOpts) -> None:
        self.qsub_script = qsub_script
        self.working_dir = working_dir
        self.jo = jo

    def get_system_command(self, env: Dict[str, str]) -> str:
        raise NotImplementedError


class SGESystemCommand(SystemCommand):
    """Class for generating system commands that submit jobs to an SGE cluster.

    Attributes:
        jo: Job options that are shared by multiple jobs.
    """

    def get_system_command(self, env: Dict[str, str]) -> str:
        """Generate a system command that can be ran to submit a job to the cluster.

        Args:
            additional_envs: Additional (job-specific) environment variables
                that should be passed to the job submission script. In the least,
                these should include `SYSTEM_COMMAND`, `STDOUT_LOG`, and `STDERR_LOG`.

        Returns:
            System command.
        """
        system_command = dedent(f"""\
            PATH="{env.get('PATH', '$PATH')}"
            qsub
            -S {self.jo.qsub_shell}
            -N {self.jo.job_name}
            {f" -M {self.jo.email} -m {self.jo.email_opts} " if self.jo.email else ""}
            -o /dev/null -e /dev/null
            -wd {self.working_dir}
            -pe smp {self.jo.nproc}
            -l h_rt={self.jo.walltime}
            {f"-l mem_free={self.jo.mem}" if self.jo.mem else ""}
            {f"-l h_vmem={self.jo.vmem}" if self.jo.vmem else ""}
            {self.get_env_string(env)}
            "{self.qsub_script}"
            {self.qsub_script_args_}
            """).replace('\n', ' ')
        return system_command

    def get_env_string(self, env: Dict[str, str]) -> str:
        return ' '.join(f'-v {d[0]}="{d[1]}"' for d in env.items())

    @property
    def qsub_script_args_(self) -> str:
        if self.jo.qsub_script_args is None:
            return ""
        else:
            return ' '.join(f'--{d[0]} {d[1]}' for d in self.jo.qsub_script_args.items())


class PBSSystemCommand(SystemCommand):
    """Class for generating system commands that submit jobs to an SGE cluster.

    Attributes:
        jo: Job options that are shared by multiple jobs.
    """

    def get_system_command(self, env: Dict[str, str]) -> str:
        """Generate a system command that can be ran to submit a job to the cluster.

        Args:
            additional_envs: Additional (job-specific) environment variables
                that should be passed to the job submission script. In the least,
                these should include `SYSTEM_COMMAND`, `STDOUT_LOG`, and `STDERR_LOG`.

        Returns:
            System command.
        """
        if ',' in env['SYSTEM_COMMAND']:
            raise Exception("Can't have commas in 'system_command' when using 'pbs'!")
        system_command = dedent(f"""\
            PATH="{env.get('PATH', '$PATH')}"
            qsub
            -S {self.jo.qsub_shell}
            -N {self.jo.job_name}
            {f" -M {self.jo.email} -m {self.jo.email_opts} " if self.jo.email else ""}
            -o /dev/null -e /dev/null
            -d {self.working_dir}
            {f"-l nodes=1:ppn={self.jo.nproc}" if self.jo.nproc else "-l nodes=1"}
            ,walltime={self.jo.walltime}
            {f",mem={self.jo.mem}" if self.jo.mem else ""}
            {f",pmem={self.jo.pmem}" if self.jo.pmem else ""}
            {f",vmem={self.jo.mem}" if self.jo.vmem else ""}
            {f",pvmem={self.jo.mem}" if self.jo.pvmem else ""}
            {self.get_env_string(env)}
            "{self.qsub_script}"
            {self.qsub_script_args_}
            """).replace('\n,', ',').replace('\n', ' ')
        return system_command

    def get_env_string(self, env: Dict[str, str] = None) -> str:
        return "-v " + ','.join(f'{d[0]}="{d[1]}"' for d in env.items())

    @property
    def qsub_script_args_(self) -> str:
        if self.jo.qsub_script_args is None:
            return ""
        else:
            return "-F " + ' '.join(f'--{d[0]} {d[1]}' for d in self.jo.qsub_script_args.items())


class SLURMSystemCommand(SystemCommand):

    def get_system_command(self, additional_envs: Dict[str, str]) -> str:
        raise NotImplementedError()
