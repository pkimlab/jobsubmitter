""".

.. note::
    Tests in this file require access to compute clusters on hard-coded IPs.
"""
import sys
import os
import os.path as op
import logging
import tempfile
import time
import paramiko
import pytest
import tarfile
import shutil
from collections import Counter
import kmtools
import jobsubmitter

logger = logging.getLogger(__name__)


def _parse_connection_string(connection_string):
    _db_info = kmtools.db_tools.parse_connection_string(connection_string)
    return _db_info['db_type'], _db_info['db_url']


def _test_ssh_connection(connection_string):
    _, ip = _parse_connection_string(connection_string)
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        ssh.connect(ip)
        return True
    except Exception as e:
        logger.error("{}: '{}'".format(type(e), str(e)))
        return False


test_input = [(connection_string, concurrent_job_limit)
              for connection_string in [('sge://:@192.168.6.201'), ('pbs://:@192.168.233.150')]
              for concurrent_job_limit in [None, 50] if _test_ssh_connection(connection_string)]
logger.info("Collected {} test inputs".format(len(test_input)))


def get_system_commands(script_filename):
    return [
        (job_id, {'system_command': '{python} \'{script}\' -i {job_id}'.format(
            python=sys.executable, script=op.join(
                op.abspath(op.dirname(__file__)), script_filename), job_id=job_id)})
        for job_id in range(99)]


@pytest.mark.skipif(pytest.config.getvalue("quick"), reason="Tests take several minutes.")
@pytest.mark.parametrize("connection_string, concurrent_job_limit", test_input)
def test_1(connection_string, concurrent_job_limit):
    """Test on tasks that finish successfully."""
    script_filename = op.join(op.dirname(op.abspath(__file__)), 'scripts', '_test_1.py')
    system_commands = get_system_commands(script_filename)
    tempdir = tempfile.TemporaryDirectory(dir=op.join(op.dirname(op.abspath(__file__)), 'jobs'))
    job_folder = tempdir.name
    # Submit jobs
    js = jobsubmitter.JobSubmitter(
        job_folder, connection_string,
        walltime='01:00:00', concurrent_job_limit=concurrent_job_limit, queue='short')
    logger.info('Submitting...')
    with js.connect():
        futures = js.submit(system_commands)
        results = [f.result() for f in futures]
    logger.debug("results: %s", results)
    logger.info('Finished submitting...')
    # Make sure that jobs finish successfully
    time_0 = time.time()
    results_df = js.job_status(system_commands)
    while not (results_df['status'] == 'done').all():
        time_1 = time.time()
        if (time_1 - time_0) > 10 * 60:
            assert False, "Timeout!"
        time.sleep(30)
        results_df = js.job_status(system_commands)
    assert True


@pytest.mark.skipif(pytest.config.getvalue("quick"), reason="Tests take several minutes.")
@pytest.mark.parametrize("connection_string, concurrent_job_limit", test_input)
def test_2(connection_string, concurrent_job_limit):
    """Test on tasks that finish in a crash."""
    script_filename = op.join(op.dirname(__file__), 'scripts', '_test_2.py')
    system_commands = get_system_commands(script_filename)
    tempdir = tempfile.TemporaryDirectory(dir=op.join(op.dirname(op.abspath(__file__)), 'jobs'))
    job_folder = tempdir.name
    # Submit jobs
    js = jobsubmitter.JobSubmitter(
        job_folder, connection_string,
        walltime='01:00:00', concurrent_job_limit=concurrent_job_limit, queue='short')
    logger.info('Submitting...')
    with js.connect():
        js.submit(system_commands)
    logger.info('Finished submitting...')
    # Make sure that jobs finish successfully
    time_0 = time.time()
    results_df = js.job_status(system_commands)
    while not (results_df['status'] == 'error').all():
        logger.info(results_df.tail())
        time_1 = time.time()
        if (time_1 - time_0) > 10 * 60:
            assert False, "Timeout!"
        time.sleep(30)
        results_df = js.job_status(system_commands)
    assert True


class TestJobStatus:

    @classmethod
    def setup_class(cls):
        cls.job_name = 'test_logs_1'
        cls.connection_string = 'sge://:@192.168.0.1'
        cls.log_base_dir = op.abspath(op.splitext(__file__)[0])
        cls.log_dir = op.join(cls.log_base_dir, cls.job_name)
        os.makedirs(cls.log_dir, exist_ok=True)
        with tarfile.open(cls.log_dir + '.tar.gz') as t:
            t.extractall(cls.log_dir)

    @classmethod
    def teardown_class(cls):
        shutil.rmtree(cls.log_dir)

    def test_job_status(self):
        js = jobsubmitter.JobSubmitter(
            self.connection_string, self.log_base_dir, self.log_base_dir, self.job_name,
            force_new_folder=False)
        results_df = js.job_status([(i, i) for i in range(3360)])
        assert (
            Counter(results_df['status']) == Counter({
                'done': 2650,
                'frozen': 387,
                'missing': 323
            }))
