# Standard Python libraries
from enum import Enum
import logging
import operator
import os
import time
import re

# Additional libraries
from paramiko import SFTPClient

# Job States
class JobState(Enum):
    Pending   = "Pending"
    Ready     = "Ready"
    Executing = "Executing"
    Completed = "Completed"

class Job:
    def __init__(self, config, prereqs=[]):
        """
        """
        self.logger = logging.getLogger("jackhammer.job")
        self.config = config
        self.shutdown = None

        # Properties to be filled in by subclass
        self.name = ""
        self.cmd = ""

        # State
        self.state = JobState.Pending
        self.attempt = 0

        # Relations to other jobs
        self.prereqs = prereqs
        self.siblings = []

        # Results
        self.result = -1
        self.response = ""

    def __repr__(self):
        return self.name

    def set_siblings(self, siblings):
        self.siblings = siblings

    def set_shutdown(self, shutdown):
        self.shutdown = shutdown

    def reset(self):
        self.state = JobState.Pending
        self.stdout = ''
        self.stderr = ''

    def prelaunch(self):
        """
        Called when the job is dequeued and inspected before
        spinning up resources. Allows for it to be avoided,
        in the event that siblings or prereqs would prevent it.
        """
        assert self.state == JobState.Pending 

        # If any siblings have already failed, bail out
        for j in self.siblings:
            if j.state == JobState.Completed and j.result > 0:
                self.state = JobState.Completed
                self.result = -1
                self.response = "SIBLING"
                return 

        # Leave as pending if any prereqs not completed
        for j in self.prereqs:
            if j.state != JobState.Completed:
                self.state = JobState.Pending
                return

        # Prepare for launch
        self.state = JobState.Ready
        self.prelaunch_hook()

    def launch(self, client):
        """
        Connect to the provided client.
        """
        self.logger.info("Launching job: %s", self)

        assert self.state == JobState.Ready
        self.state = JobState.Executing

        try:
            self.sendFiles(client)
        except Exception as e:
            self.logger.error("Failed to send files: %s" % str(e))
            self.state = JobState.Completed
            self.result = 1
            return

        try:
            self.runCommand(client)
        except Exception as e:
            self.logger.error("Failed to run cmd: " + str(e))
            self.state = JobState.Completed
            self.result = 1

    def postlaunch(self):
        """
        Interpret results of execution.
        Returns a list of jobs to schedule, which can include self.
        """
        self.logger.info("Stopping job: %s", self)

        assert self.state == JobState.Completed

        if self.result == 0:
            return self.completed_hook()
        elif self.result == -1:
            if self.attempt < self.config['attempts']:
                self.attempt += 1
                return [self]
            return self.failed_hook()
        else:
            return self.failed_hook()

    def sendFiles(self, client):
        """
        Send the script to the remote machine.
        """
        sftp = SFTPClient.from_transport(client.get_transport())
        for (src, dst) in self.config['files']:
            self.logger.debug("Sending file %s -> %s" % (src, dst))
            sftp.put(src, dst)
        sftp.close()

    def runCommand(self, client):
        """
        Launch the script, stashing its output and checking the
        return code.
        """
        # Open a connection and run the command
        self.logger.debug("Launching command: %s", self.cmd)
        transport = client.get_transport()
        channel = transport.open_session()
        channel.exec_command(self.cmd)

        # Loop to capture results
        self.stdout = ''
        self.stderr = ''
        lastLine = ''
        while not self.shouldShutdown(channel):
            lastLine = self.collect_output(channel, lastLine)
            time.sleep(1)
        self.collect_output(channel, lastLine, True)

        # Determine result, either using logs or status code
        self.collect_result(channel)
        channel.close()

    def shouldShutdown(self, channel):
        if channel != None and channel.exit_status_ready():
            return True
        if self.state == JobState.Completed:
            return True
        if self.shutdown != None and self.shutdown.is_set():
            self.state = JobState.Completed
            self.result = -1
            self.response = "SHUTDOWN"
            return True

    def collect_result(self, channel):
        """
        Collect the commands result, either by interpreting known
        values in the results or based on its return status.
        """
        if self.state == JobState.Completed:
            self.logger.debug("Got job result from comms: %s %s", self.result, self.response)
        else:
            self.state = JobState.Completed
            self.response = ""
            self.result = channel.recv_exit_status()
            if self.result == 0:
                self.logger.debug("Server returned a zero status, completed")
            elif self.result == -1:
                self.logger.error("Server did not return a status, failed")
                self.response = "PREEMPTED"
            else:
                self.logger.error("Server return non-zero status: %d", self.result)

    def collect_output(self, channel, lastLine, final=False):
        """
        Process output, line by line.
        """
        while channel.recv_ready():
            l = channel.recv(128).decode('ascii', errors="ignore")
            lines = l.split('\n')
            lines[0] = lastLine + lines[0]
            lastLine = lines[-1]
            for line in (lines if final else lines[:-1]):
                self.parse_output_line(line)
                self.stdout += line + '\n'
        while channel.recv_stderr_ready():
            e = channel.recv_stderr(128).decode('ascii', errors="ignore")
            self.stderr += e
        return lastLine

    def parse_output_line(self, line):
        prefix = ["Executing", "Completed", "Failed"]
        for s in prefix:
            r = "^%s(.*)" % s
            if re.search(r, line):
                response = re.search(r, line).group(1).strip()
                if s in ["Completed", "Failed"]:
                    self.state = JobState.Completed
                    self.result = 0 if s == "Completed" else 1
                    self.response = response
                self.logger.debug("%s: %s" % (self.state, response))
                return
        self.parse_hook(line)

    # Hooks
    def prelaunch_hook(self):
        """
        Hook called when the job is removed from the queue and
        prepared for launch.
        """
        pass

    def parse_hook(self, line):
        """
        Hook to parse a line from the remote worker.
        Allows for the implementation of custom progress notications.
        """
        return

    def completed_hook(self):
        """
        Hook in the event of a completed job.
        Can generate further jobs.
        """
        return []

    def failed_hook(self):
        """
        Hook in the event of a failed job.
        Can generate further jobs.
        """
        return []
