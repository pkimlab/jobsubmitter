import json
import logging
import os.path as op

import pytest

import jobsubmitter
from conftest import PATH

logger = logging.getLogger(__name__)


# @pytest.fixture(scope='session', params=])
# def executor(request):
#     return request.param



from pathlib import Path

Path('path/to/file.txt').touch()


def submit_jobs(system_commands, executor):
    job_folder = op.join(op.dirname(op.abspath(__file__)), 'jobs')
    logger.debug("job_folder: %s", job_folder)
    js = jobsubmitter.JobSubmitter(
        job_folder,
        executor,
        #
        nproc=1,
        queue='medium',
        walltime='01:00:00',
        mem='1700M',
        env={'PATH': PATH},
    )
    logger.debug("js.job_abspath: %s", js.job_abspath)
    with js.connect():
        futures = js.submit(system_commands.items())
        logger.debug("futures: %s", futures)
        logger.debug("results: %s", [f.result() for f in futures])
    logger.debug("Status:\n%s", js.job_status(system_commands.items()))


@pytest.mark.parametrize("executor", ['local', 'beagle', 'banting'])
def test_simple_1(executor):
    system_commands = {
        1: {'system_command': "echo 'hello world'", 'random things': 'totally'},
    }
    submit_jobs(system_commands, executor)


@pytest.mark.parametrize("executor", ['local', 'beagle'])
def test_simple_2(executor):
    """This test fails on banting because PBS does not allow commas in system commands."""
    data = {'a': 10, 'b': 20, 'c': 30}
    system_commands = {
        2: {'system_command': "echo '{}'".format(json.dumps(data))},
    }
    submit_jobs(system_commands, executor)
