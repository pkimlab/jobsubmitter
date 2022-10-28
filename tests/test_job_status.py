import logging
import os.path as op
import tarfile
from collections import Counter
from pathlib import Path

import pandas as pd
import pytest
from conftest import PATH

import jobsubmitter

logger = logging.getLogger(__name__)


@pytest.fixture
def job_opts(tmpdir):
    job_opts = jobsubmitter.JobOpts(
        job_id='job_0',
        working_dir=Path(tmpdir),
        nproc=1,
        queue='medium',
        walltime='01:00:00',
        mem='1700M',
        env={'PATH': PATH},
    )
    job_dir = job_opts.working_dir.joinpath(job_opts.job_id)
    job_logs_file = Path(op.abspath(op.splitext(__file__)[0])).joinpath('test_logs_1.tar.gz')
    with tarfile.open(job_logs_file) as t:
        def is_within_directory(directory, target):
            
            abs_directory = os.path.abspath(directory)
            abs_target = os.path.abspath(target)
        
            prefix = os.path.commonprefix([abs_directory, abs_target])
            
            return prefix == abs_directory
        
        def safe_extract(tar, path=".", members=None, *, numeric_owner=False):
        
            for member in tar.getmembers():
                member_path = os.path.join(path, member.name)
                if not is_within_directory(path, member_path):
                    raise Exception("Attempted Path Traversal in Tar File")
        
            tar.extractall(path, members, numeric_owner=numeric_owner) 
            
        
        safe_extract(t, job_dir)
    return job_opts


@pytest.mark.parametrize("host", ['local://localhost'])
def test_job_status(host, job_opts):
    df = pd.DataFrame(
        data=[[f"echo '{i}'"] for i in range(3360)],
        columns=['system_command'],
        index=list(range(3360)),
    )
    js = jobsubmitter.JobSubmitter(host)
    results = js.job_status(df, job_opts, progressbar=False)
    assert Counter(results['status']) == Counter({'done': 2650, 'frozen': 387, 'missing': 323})
