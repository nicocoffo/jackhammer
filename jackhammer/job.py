# Standard Python libraries
import uuid
from enum import Flag, auto
from logging import getLogger

# Package libraries
from jackhammer.utility import send_files, run_command, send_script

logger = getLogger("jackhammer")


class JobState(Flag):
    Pending = auto()
    Ready = auto()
    Success = auto()
    Disconnection = auto()
    Failure = auto()
    Complete = Success | Disconnection | Failure


class Job:
    """
    Description of a job.
    Sends a series of files to the remote machine and executes a command.
    """

    def __init__(self, config, prereqs=[]):
        # Args
        self.config = config
        self.prereqs = prereqs
        self.priority = 10

        # State
        self.name = ''
        self.script = ''
        self.script_name = '/tmp/jackhammer.sh'
        self.cmd = 'bash ' + self.script_name
        self.resetCount = -1
        self.reset()

        self.work_dir = "/tmp/jackhammer-" + str(uuid.uuid4())

    def reset(self):
        """
        Reset the job's state, such as its logs and exception.
        """
        self.state = JobState.Pending
        self.stderr = ''
        self.stdout = ''
        self.exception = None
        self.resetCount += 1

    def retry(self):
        """
        Determine if the job should be retried if it ended with
        a state other than success.
        """
        return self.state == JobState.Disconnection and \
                self.resetCount < self.config['maxAttempts']

    def prepare(self):
        """
        Called by the scheduler thread to promote a job 
        to Ready. Can skip execution entirely, based
        on dependencies.
        """
        assert self.state == JobState.Pending

        # Leave as pending if any prereqs not completed
        for j in self.prereqs:
            if not (j.state & JobState.Complete):
                return

        # Prepare for execution
        self.state = JobState.Ready

    def execute(self, client, shutdown_flag):
        """
        Called from the assigned worker thread with a connection
        to a remote machine. Performs the actual job, first sending
        files and then executing the command.
        """
        logger.debug("Job Launch: %s", self)
        assert self.state == JobState.Ready

        try:
            send_files(client, self.config['files'])
            send_script(client, self.script, self.script_name)
        except Exception as e:
            self.state = JobState.Failure
            self.exception = e
            return

        try:
            code = run_command(client, self.cmd, shutdown_flag,
                               self.process_stdout, self.process_stderr)
            if code == 0:
                self.state = JobState.Success
            elif code == -1:
                self.state = JobState.Disconnection
            else:
                self.state = JobState.Failure
        except Exception as e:
            self.state = JobState.Failure
            self.exception = e

    def cleanup(self):
        """
        Called from the scheduler thread to cleanup any completed
        jobs. 
        """
        assert self.state & JobState.Complete, \
                "Cleanup attempted on incomplete job: %s" % self

        if self.state == JobState.Success:
            logger.debug("Job Cleanup: %s", self)
            return self.success()
        elif self.retry():
            logger.warning("Job Retry: %s", self)
            self.reset()
            return [self]
        else:
            self.state = JobState.Failure
            logger.error("Job Failed: %s %s", self, self.state)
            logger.debug("STDOUT:\n%s", self.stdout)
            logger.debug("STDERR:\n%s", self.stderr)
            return self.failure()

    def success(self):
        """
        Called during cleanup for a successful job.
        """
        return []

    def failure(self):
        """
        Called during cleanup for a failed job.
        """
        return []

    def process_stdout(self, stdout):
        """
        Callback for processing stdout chunks from the command.
        """
        self.stdout += stdout

    def process_stderr(self, stderr):
        """
        Callback for processing stderr chunks from the command.
        """
        self.stderr += stderr

    def report(self):
        """
        Simple report of STDOUT and STDERR.
        """
        return "STDOUT:\n%s\nSTDERR:\n%s" % (self.stdout, self.stderr)

    def __lt__(self, other):
        return self.priority < other.priority

    def __repr__(self):
        return self.name
