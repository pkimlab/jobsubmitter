# jobsubmitter

[![anaconda](https://anaconda.org/ostrokach/jobsubmitter/badges/version.svg)](https://anaconda.org/ostrokach/jobsubmitter)
[![docs](https://img.shields.io/badge/docs-latest-blue.svg?style=flat-square&?version=latest)](http://kimlab.gitlab.io/jobsubmitter)
[![build status](https://gitlab.com/ostrokach/jobsubmitter/badges/master/build.svg)](https://gitlab.com/kimlab/jobsubmitter/commits/master)
[![coverage report](https://gitlab.com/ostrokach/jobsubmitter/badges/master/coverage.svg)](https://gitlab.com/kimlab/jobsubmitter/commits/master)

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
