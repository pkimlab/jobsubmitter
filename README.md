# Job Submitter


[![conda](https://img.shields.io/conda/dn/ostrokach/jobsubmitter.svg)](https://anaconda.org/ostrokach/jobsubmitter/)
[![docs](https://img.shields.io/badge/docs-v0.0.2-blue.svg)](https://ostrokach.gitlab.io/jobsubmitter/)
[![build status](https://gitlab.com/ostrokach/jobsubmitter/badges/master/build.svg)](https://gitlab.com/ostrokach/jobsubmitter/commits/master/)
[![coverage report](https://gitlab.com/ostrokach/jobsubmitter/badges/master/coverage.svg)](https://gitlab.com/ostrokach/jobsubmitter/commits/master/)

Package for running jobs on Sun Grid Engine (SGE) / PBS / Slurm clusters.

## Goals

- Provide an easy way to submit batch jobs from within a Jupyter notebook running on one of the nodes in the cluster.

## Example

```python
from jobsubmitter import JobOpts, JobSubmitter

JOB_ID = 'job_0'

ENV = {
    'PATH': '/home/kimlab1/strokach/anaconda/bin:/usr/local/bin:/usr/bin:/bin',
    'OMP_NUM_THREADS': '1',
}

jo = jobsubmitter.JobOpts(
    job_id=JOB_ID,
    working_dir=Path.cwd(),
    nproc=1,
    queue='medium',
    walltime='24:00:00',
    mem='16G',
    env=ENV,
)
js = jobsubmitter.JobSubmitter('localhost')

futures = js.submit(system_commands, jo, deplay=0.1)
```

## Contributing

- Make sure all tests pass before merging into master.
- Follow the PEP8 / PyFlake / Flake8 / etc. guidelines.
- Add tests for new code.
- Try to document things.
- Break any / all of the above if you have a good reason.
