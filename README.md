# jobsubmitter

[![anaconda](https://anaconda.org/ostrokach/jobsubmitter/badges/version.svg)](https://anaconda.org/ostrokach/jobsubmitter)
[![docs](https://img.shields.io/badge/docs-latest-blue.svg?style=flat-square&?version=latest)](http://ostrokach.gitlab.io/jobsubmitter)
[![build status](https://gitlab.com/ostrokach/jobsubmitter/badges/master/build.svg)](https://gitlab.com/ostrokach/jobsubmitter/commits/master)
[![coverage report](https://gitlab.com/ostrokach/jobsubmitter/badges/master/coverage.svg)](https://gitlab.com/ostrokach/jobsubmitter/commits/master)

Package for running jobs on Sun Grid Engine (SGE) / Torque / Slurm clusters.

## Goals

- Provide an easy way to submit batch jobs from within a Jupyter notebook running on one of the nodes in the cluster.

## Example

```python
import jobsubmitter

JOB_ID = 'job_0'
DATA_ID = 'adjacency_matrix_3.parquet'

JOB_DIR = Path(f"~/datapkg/{os.environ['DB_SCHEMA']}/notebooks/{NOTEBOOK_NAME}/{JOB_ID}")
DATA_DIR = Path(f"~/datapkg/{os.environ['DB_SCHEMA']}/notebooks/{NOTEBOOK_NAME}/{DATA_ID}")

ENV = {
    'PATH': '/home/kimlab1/strokach/anaconda/bin:/usr/local/bin:/usr/bin:/bin',
    'OMP_NUM_THREADS': '1',
    'OPENMM_CPU_THREADS': '1',
}

js = jobsubmitter.JobSubmitter('beagle', JOB_DIR, DATA_DIR, ENV)
jo = jobsubmitter.JobOpts(JOB_ID, nproc=1, queue='medium', walltime='48:00:00', mem='16G')

futures = js.submit(system_commands, jo, deplay=0.1)
```

## Contributing

- Make sure all tests pass before merging into master.
- Follow the PEP8 / PyFlake / Flake8 / etc. guidelines.
- Add tests for new code.
- Try to document things.
- Break any / all of the above if you have a good reason.
