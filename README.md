# jobsubmitter

[![anaconda](https://anaconda.org/kimlab/jobsubmitter/badges/version.svg?style=flat-square)](https://anaconda.org/ostrokach/jobsubmitter)
[![docs](https://img.shields.io/badge/docs-latest-blue.svg?style=flat-square&?version=latest)](http://kimlaborg.github.io/jobsubmitter)
[![travis](https://img.shields.io/travis/kimlaborg/jobsubmitter.svg?style=flat-square)](https://travis-ci.org/kimlaborg/jobsubmitter)
[![codecov](https://img.shields.io/codecov/c/github/kimlaborg/jobsubmitter.svg?style=flat-square)](https://codecov.io/gh/kimlaborg/jobsubmitter)

Package for running jobs on Sun Grid Engine (SGE) / Torque / PBS.


## Example

```python
import os.path as op

# Initialize JobSubmitter
js = jobsubmitter.JobSubmitter(
    job_name='test',
    connection_string='sge://:@192.168.XXX.XXX',
    log_root_path=op.expanduser('~/pbs_output'),
    email='noname@example.com',
    force_new_folder=False,
    concurrent_job_limit=None,  # max number of jobs to submit at a time
    nproc=1, queue='medium', walltime='8:00:00', mem='6G',
    env={'PATH': '/home/username/anaconda/bin'}
)

# Submit jobs
with js.connect():
    js.submit([0, "echo 'hello world'"])

# Monitor jobs
with js.connect():
    print(js.get_num_running_jobs())

# Read job results
results = js.job_status()
print(Counter(results['status']))
```


## Contributing

- Make sure all tests pass before merging into master.
- Follow the PEP8 / PyFlake / Flake8 / etc. guidelines.
- Add tests for new code.
- Try to document things.
- Break any / all of the above if you have a good reason.
