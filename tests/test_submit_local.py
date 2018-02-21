import json
import logging
from pathlib import Path

import pandas as pd
import pytest

import jobsubmitter
from conftest import PATH

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
    return job_opts


@pytest.mark.parametrize("host", ['local://localhost'])
def test_submit_local_1(host, job_opts):
    df = pd.DataFrame(
        data=["echo 'hello world'"],
        columns=['system_command'],
        index=[1],
    )
    js = jobsubmitter.JobSubmitter(host)
    futures = js.submit(df, job_opts, progressbar=False)
    assert futures
    results = js.job_status(df, job_opts, progressbar=False)
    assert (results['status'] == 'done').all()
    assert results.at[1, 'stdout_data'] == 'hello world'


@pytest.mark.parametrize("host", ['local://localhost'])
def test_submit_local_2(host, job_opts):
    """This test fails on banting because PBS does not allow commas in system commands."""
    data = {'a': 10, 'b': 20, 'c': 30}
    df = pd.DataFrame(
        data=["echo '{}'".format(json.dumps(data))],
        columns=['system_command'],
        index=[2],
    )
    js = jobsubmitter.JobSubmitter(host)
    futures = js.submit(df, job_opts, progressbar=False)
    assert futures
    results = js.job_status(df, job_opts, progressbar=False)
    assert (results['status'] == 'done').all()
    assert all(results.at[2, k] == v for k, v in data.items())
