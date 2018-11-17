import time
import re
from paramiko import SFTPClient


def send_files(client, files):
    """
    Send a list of files to the remote, formatted as a list
    of (src, dst) pairs.
    """
    sftp = SFTPClient.from_transport(client.get_transport())
    for (src, dst) in files:
        sftp.put(src, dst)
    sftp.close()


def collect_output(channel, stdout, stderr):
    """
    Helper for run_command to collect bytes from the channel's
    stdout and stderr.
    """
    while channel.recv_ready():
        stdout += channel.recv(1).decode('ascii', errors="ignore")
    while channel.recv_stderr_ready():
        stderr += channel.recv_stderr(1).decode('ascii', errors="ignore")
    return stdout, stderr


def run_command(client, cmd, shutdown):
    """
    Run a command on a remote client, collecting the
    return status, stdout and stderr. Also support a shutdown flag
    to perform an early disconnection.
    """
    transport = client.get_transport()
    channel = transport.open_session()
    channel.exec_command(cmd)

    # Results
    stdout = ''
    stderr = ''
    code = None

    # Loop to capture results
    while not shutdown.is_set() and not channel.exit_status_ready():
        stdout, stderr = collect_output(channel, stdout, stderr)
        time.sleep(1)

    # Get last of the output and code if available
    stdout, stderr = collect_output(channel, stdout, stderr)
    code = channel.recv_exit_status() if channel.exit_status_ready() else -1
    channel.close()
    return code, stdout, stderr
