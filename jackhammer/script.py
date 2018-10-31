# Standard Python libraries
import logging
import os
import time

# Additional libraries
from paramiko import SFTPClient

DEFAULT_TIMEOUT=(60 * 60 * 12)

class Script:
    """Launch a script on the client.
    
    This class encapsulates the task of sending
    and launching a script on the remote machine
    over an SSH connection.
    """

    def __init__(self, script, args=[], name=None):
        self.logger = logging.getLogger("jackhammer.script")

        # Strip the script's components
        self.script = script
        self.path, self.file = os.path.split(script)
        self.name = self.file if name == None else name

        # Generate the command
        l = ['"' + a + '"' for a in args]
        self.cmd = self.file + " " + " ".join(l)

    def send(self, client, tmpScript):
        """
        Send the script to the remote machine.
        """
        sftp = SFTPClient.from_transport(client.get_transport())
        try:
            sftp.put(self.script, tmpScript)
        finally:
            sftp.close()

    def launch(self, client, cmd, timeout):
        """
        Launch the script, stashing its output and checking the
        return code.
        """
        stdin, stdout, stderr = client.exec_command(cmd, get_pty=True)
        stdout._set_mode('b')
        channel = stdout.channel

        self.logs = b''
        while not channel.exit_status_ready():
            if channel.recv_ready():
                self.logs += channel.recv(2048)
            time.sleep(1)
            timeout -= 1
            if timeout <= 0:
                return False

        return channel.recv_exit_status() == 0

    def execute(self, client, timeout=DEFAULT_TIMEOUT):
        """
        Send the script and launch it.
        Returns True on successful execution.
        """
        tmpScript = "/tmp/" + self.file
        cmd = "bash /tmp/" +  self.cmd

        self.logger.info("Sending file %s -> %s" % (self.script, tmpScript))
        try:
            self.send(client, tmpScript)
        except Exception as e:
            self.logger.error("Failed to send script: " + str(e))
            return False

        self.logger.info("Launching script: %s" % cmd)
        try:
            result = self.launch(client, cmd, timeout)
        except Exception as e:
            self.logger.error("Failed to run script: " + str(e))
            return False

        if result != True:
            self.logger.error("Execution failed: \n %s" % self.logs)
        else:
            self.logger.info("Execution succeeded")

        return result

    def __repr__(self):
        return self.cmd
