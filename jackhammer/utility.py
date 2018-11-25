import time
import re
import socket
from paramiko import SFTPClient
from paramiko.sftp_file import SFTPFile


def send_files(client, files):
    """
    Send a list of files to the remote, formatted as a list
    of dicts, with local and remote entries.
    """
    sftp = SFTPClient.from_transport(client.get_transport())
    for conf in files:
        sftp.put(conf['local'], conf['remote'])
    sftp.close()


def send_script(client, content, dst):
    """
    Send a list of files to the remote, formatted as a list
    of dicts, with local and remote entries.
    """
    sftp = SFTPClient.from_transport(client.get_transport())
    with SFTPFile(sftp, dst, mode='w') as f:
        f.write(content)
    sftp.close()


def collect_output(channel, stdout, stderr):
    """
    Helper for run_command to collect bytes from the channel's
    stdout and stderr.
    """
    try:
        while channel.recv_ready() and stdout:
            stdout(channel.recv(512).decode('ascii', errors="ignore"))
    except socket.timeout:
        pass

    try:
        while channel.recv_stderr_ready() and stderr:
            stderr(channel.recv_stderr(512).decode('ascii', errors="ignore"))
    except socket.timeout:
        pass


def run_command(client, cmd, shutdown, stdout=None, stderr=None):
    """
    Run a command on a remote client and get the return status.
    stdout and stderr can be processed using the callbacks.
    A shutdown flag allows for early disconnection.
    """
    transport = client.get_transport()
    channel = transport.open_session()
    channel.setblocking(0)
    channel.exec_command(cmd)

    # Results
    code = None

    # Loop to capture results
    while not shutdown.is_set() and not channel.exit_status_ready():
        collect_output(channel, stdout, stderr)
        time.sleep(1)

    # Get last of the output and code if available
    collect_output(channel, stdout, stderr)
    code = channel.recv_exit_status() if channel.exit_status_ready() else -1
    channel.close()
    return code
