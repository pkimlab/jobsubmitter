import logging

import paramiko

from kmtools import system_tools

logger = logging.getLogger(__name__)


@system_tools.retry_ssh
def execute_remotely(system_command: str, ssh: paramiko.SSHClient) -> str:
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
