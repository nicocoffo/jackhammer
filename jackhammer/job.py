# Standard Python libraries
from enum import Enum
import logging
import operator
import os
import time
import re

# Additional libraries
from paramiko import SFTPClient

DEFAULT_TIMEOUT=(60 * 60 * 12)

# Job States
class JobState(Enum):
    Queued    = "Queued"
    Executing = "Executing"
    Completed = "Completed"
    Failed    = "Failed"

class JobConfig:
    def __init__(self, attempts=3, connection={}, machine={}, files=[]):
        self.connection = connection
        self.machine = machine
        self.files = files
        self.attempts = attempts

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
        self.state = JobState.Queued
        self.attempt = 0

        # Relations to other jobs
        self.prereqs = prereqs
        self.siblings = []

        # Results
        self.response = ""

    def __repr__(self):
        return self.name

    def set_siblings(self, siblings):
        self.siblings = siblings

    def set_shutdown(self, shutdown):
        self.shutdown = shutdown

    def prelaunch(self):
        """
        Called when the job is dequeued and inspected before
        spinning up resources. Allows for it to be avoided,
        in the event that siblings or prereqs would prevent it.
        """
        siblings = [j.state for j in self.siblings]
        prereqs = [j.state for j in self.prereqs]

        if JobState.Failed in siblings:
            # Check if any siblings have already failed
            self.state = JobState.Failed
        elif JobState.Queued in prereqs or JobState.Executing in prereqs:
            # Check if any prereqs are still waiting
            self.state = JobState.Queued
        else:
            # Prepare for launch
            self.state = JobState.Executing
            self.prelaunch_hook()

    def launch(self, client):
        """
        Connect to the provided client.
        """
        # If prelaunch has run, state should be configured
        if self.state != JobState.Executing:
            raise Exception("Unexpected prelaunch state: %s" % self.state)

        # Send all the necessary files, based on config
        try:
            self.sendFiles(client)
        except Exception as e:
            self.logger.error("Failed to send files: %s" % str(e))
            self.state = JobState.Failed
            return

        # Run the command
        try:
            self.runCommand(client)
        except Exception as e:
            self.logger.error("Failed to run cmd: " + str(e))
            self.state = JobState.Failed

    def postlaunch(self):
        """
        Interpret results of execution.
        Returns a list of jobs to schedule, which can include self.
        """
        # Call the hook based on the job state
        if self.state == JobState.Completed:
            return self.completed_hook()
        elif self.state == JobState.Failed:
            self.logger.info("STDOUT: %s", self.stdout)
            self.logger.info("STDERR: %s", self.stderr)
            if self.response == "PREEMPTED":
                if self.attempt < self.config.attempts:
                    self.attempt += 1
                    return [self]
            return self.failed_hook()
        else:
            raise Exception("Unexpected postlaunch state: %s" % self.state)

    def sendFiles(self, client):
        """
        Send the script to the remote machine.
        Issues: Need to implement a timeout.
        """
        sftp = SFTPClient.from_transport(client.get_transport())
        for (src, dst) in self.config.files:
            self.logger.info("Sending file %s -> %s" % (src, dst))
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
        if self.state in [JobState.Completed, JobState.Failed]:
            return True
        if self.shutdown != None and self.shutdown.is_set():
            self.state = JobState.Failed
            self.response = "SHUTDOWN"
            return True

    def collect_result(self, channel):
        """
        Collect the commands result, either by interpreting known
        values in the results or based on its return status.
        """
        if self.state in [JobState.Completed, JobState.Failed]:
            self.logger.info("Got job result from comms: %s %s", self.state, self.response)
        else:
            status = channel.recv_exit_status()
            if status == 0:
                self.logger.info("Server returned a zero status, completed")
                self.response = ""
                self.state = JobState.Completed
                return
            elif status == -1:
                self.logger.error("Server did not return a status, failed")
                self.response = "PREEMPTED"
            else:
                self.logger.error("Server return non-zero status: %d", status)
                self.response = str(status)
            self.state = JobState.Failed

    def collect_output(self, channel, lastLine, final=False):
        """
        Process output, line by line.
        """
        while channel.recv_ready():
            l = channel.recv(128).decode('ascii', errors="ignore")
            lines = l.split('\n')
            lines[0] = lastLine + lines[0]
            lastLine = '' if len(lines) == 1 else lines[-1]
            for line in (lines if final else lines[:-1]):
                self.parse_output_line(line)
                self.stdout += line + '\n'
        while channel.recv_stderr_ready():
            e = channel.recv_stderr(128).decode('ascii', errors="ignore")
            self.stderr += e
        return lastLine

    def parse_output_line(self, line):
        prefix = [JobState.Executing, JobState.Completed, JobState.Failed]
        for s in [e.name for e in prefix]:
            r = "^%s(.*)" % s
            if re.search(r, line):
                response = re.search(r, line).group(1).strip()
                self.state = JobState(s)
                if self.state in [JobState.Completed, JobState.Failed]:
                    self.response = response
                self.logger.info("%s: %s" % (self.state, response))
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
