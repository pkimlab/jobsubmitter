import functools
import logging

import paramiko
from retrying import retry

logger = logging.getLogger(__name__)


def retry_ssh(fn):
    """Retry doing something over an ssh connection."""
    _check_exception = functools.partial(check_exception, valid_exc=paramiko.SSHException)
    wrapper = retry(
        retry_on_exception=_check_exception,
        wait_exponential_multiplier=1_000,
        wait_exponential_max=60_000,
        stop_max_attempt_number=7)
    return wrapper(fn)


def check_exception(exc, valid_exc):
    logger.error('The following exception occured:\n{}'.format(exc))
    to_retry = isinstance(exc, valid_exc)
    if to_retry:
        logger.error('Retrying...')
    return to_retry


@retry_ssh
def execute_remotely(ssh: paramiko.SSHClient, system_command: str) -> str:
    """Execute a system command on a remote server.

    Returns:
        STDOUT from the remote execution.
        We do not return STDERR because we treat the presence of STDERR as an error
        (this is why this function is not very generalizable).
    """
    logger.debug("system_command: '%s'", system_command)
    stdin_fh, stdout_fh, stderr_fh = ssh.exec_command(system_command, get_pty=True)
    stdout = stdout_fh.read().decode().strip()
    stderr = stderr_fh.read().decode().strip()
    if stdout:
        logger.debug(stdout)
    if stderr:
        logger.warning(stderr)
        raise paramiko.ChannelException(0, stderr)
    return stdout
