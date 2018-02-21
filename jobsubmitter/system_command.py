from textwrap import dedent

from .job_opts import JobOpts


def get_system_command(cluster: str, job_opts: JobOpts) -> str:
    if cluster == 'sge':
        return _get_sge_system_command(job_opts)
    elif cluster == 'pbs':
        return _get_pbs_system_command(job_opts)
    elif cluster == 'slurm':
        return _get_slurm_system_command(job_opts)
    else:
        raise ValueError(f"Unknown cluster type: {cluster}.")


def _get_sge_system_command(jo: JobOpts) -> str:

    system_command = dedent(f"""\
        PATH="{jo.env.get('PATH', '$PATH')}"
        qsub
        -S {jo.qsub_shell}
        -N {jo.job_id}
        -o /dev/null -e /dev/null
        -wd {jo.working_dir}
        -pe smp {jo.nproc}
        -l h_rt={jo.walltime}
        {f"-l mem_free={jo.mem}" if jo.mem else ""}
        {f"-l h_vmem={jo.vmem}" if jo.vmem else ""}
        {f"-l gpu={jo.gpus}" if jo.gpus else ""}
        {f"-t {jo.array_jobs.partition('%')[0]}" if jo.array_jobs else ""}
        {f"-tc {jo.array_jobs.partition('%')[-1]}" if '%' in jo.array_jobs else ""}
        {f" -M {jo.email} -ma" if jo.email else ""}
        {' '.join(f'-v {key}="{value}"' for key, value in jo.env.items())}
        "{jo.qsub_script}"
        """).replace('\n', ' ')
    return system_command


def _get_pbs_system_command(jo: JobOpts) -> str:
    """Generate a system command that can be ran to submit a job to the cluster.

    Returns:
        System command.
    """
    assert ',' not in jo.env['SYSTEM_COMMAND']

    system_command = dedent(f"""\
        PATH="{jo.env.get('PATH', '$PATH')}"
        qsub
        -S {jo.qsub_shell}
        -N {jo.job_id}
        -o /dev/null -e /dev/null
        -d {jo.working_dir}
        -l nodes=1
        {f":ppn={jo.nproc}" if jo.nproc else ""}
        {f":gpus={jo.gpus}" if jo.gpus else ""}
        ,walltime={jo.walltime}
        {f",mem={jo.mem}" if jo.mem else ""}
        {f",pmem={jo.pmem}" if jo.pmem else ""}
        {f",vmem={jo.mem}" if jo.vmem else ""}
        {f",pvmem={jo.mem}" if jo.pvmem else ""}
        {f"-t {jo.array_jobs}" if jo.array_jobs else ""}
        {f"-A {jo.account}" if jo.account else ""}
        {f"-M {jo.email} -ma" if jo.email else ""}
        {"-v " + ','.join(f'{key}="{value}"' for key, value in jo.env.items())}
        "{jo.qsub_script}"
        """).replace('\n', ' ')
    return system_command


def _get_slurm_system_command(jo: JobOpts) -> str:
    assert ',' not in jo.env['SYSTEM_COMMAND']
    system_command = dedent(f"""\
        PATH="{jo.env.get('PATH', '$PATH')}"
        sbatch
        -o /dev/null -e /dev/null
        --job-name={jo.job_id}
        --workdir="{jo.working_dir}"
        --cpus-per-task={jo.nproc}
        --time={jo.walltime}
        --mem={jo.mem}
        {f"--gres=gpu:{jo.gpus}" if jo.gpus else ""}
        {f"--array={jo.array_jobs}" if jo.array_jobs else ""}
        {f"--account={jo.account}" if jo.account else ""}
        {f"--mail-user={jo.email} --mail-type=FAIL" if jo.email else ""}
        {"--export=" + ','.join(f'{key}="{value}"' for key, value in jo.env.items())}
        "{jo.qsub_script}"
        """).replace('\n', ' ')
    return system_command
