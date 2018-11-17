# Standard Python libraries
from enum import Enum
import logging

# SSH Utilities
from jackhammer.utility import send_files, run_command

# Job States - Stages of progress the job goes through
class JobState(Enum):
    Pending   = 0
    Ready     = 1
    Completed = 2

# Job Status - The result of the job
class JobStatus(Enum):
    Unknown           = 0
    Success           = 1
    ScriptFailure     = 2
    DependencyFailure = 3
    Disconnection     = 4

class Job:
    def __init__(self, config, prereqs=[]):
        """
        """
        self.logger = logging.getLogger("jackhammer.job")
        self.config = config

        # State
        self.name = ""
        self.cmd = ""
        self.attempts = 0
        self.reset()

        # Relations to other jobs
        self.prereqs = prereqs
        self.siblings = []

    def set_siblings(self, siblings):
        self.siblings = siblings

    def set_status(self, status, msg=""):
        self.state = JobState.Completed
        self.status = status
        if msg != "":
            self.stderr += "\n" + msg + "\n"

    def reset(self):
        self.state = JobState.Pending
        self.status = JobStatus.Unknown
        self.stderr = ""
        self.stdout = ""

    def failed(self):
        return self.state == JobState.Completed and \
                self.status != JobStatus.Success

    def retry(self):
        return self.status == JobStatus.Disconnection and \
                self.attempts < self.config['maxAttempts']

    def prepare(self):
        """
        Called from the scheduler thread to promote a job from
        Pending to Ready. Can skip execution entirely, based
        on dependencies.
        """
        assert self.state == JobState.Pending 

        # If any siblings have already failed, bail out
        for j in self.siblings:
            if j.failed():
                self.set_status(JobStatus.DependencyFailure)
                return 

        # Leave as pending if any prereqs not completed
        for j in self.prereqs:
            if j.state != JobState.Completed:
                self.state = JobState.Pending
                return

        # Prepare for execution
        self.state = JobState.Ready
        self.prepare_hook()

    def execute(self, client, shutdown):
        """
        Called from the assigned worker thread with a connection
        to a remote machine. Performs the actual job, first sending
        files and then executing the command.
        """
        self.logger.info("Launching job: %s", self)
        assert self.state == JobState.Ready

        try:
            send_files(client, self.config['files'])
        except Exception as e:
            self.set_status(JobStatus.Unknown, str(e))
            return

        try:
            code, self.stdout, self.stderr = \
                    run_command(client, self.cmd, shutdown)
            if code == 0:
                self.set_status(JobStatus.Success)
            elif code == -1:
                self.set_status(JobStatus.Disconnection)
            else:
                self.set_status(JobStatus.ScriptFailure)
        except Exception as e:
            self.set_status(JobStatus.Unknown, str(e))

    def cleanup(self):
        """
        Called from the scheduler thread to cleanup any completed
        jobs. 
        """
        self.logger.info("Cleaning up job: %s", self)
        assert self.state == JobState.Completed

        if self.status == JobStatus.Success:
            return self.success_hook()
        elif self.retry():
            self.logger.warning("Job retry: %s %s", self, self.status)
            self.attempts += 1
            self.reset()
            return [self]
        else:
            self.logger.error("Job failed: %s %s", self, self.status)
            self.logger.debug("STDOUT:\n%s", self.stdout)
            self.logger.debug("STDERR:\n%s", self.stderr)
            return self.failure_hook()

    # Hooks
    def prepare_hook(self):
        """
        Hook called during prepare.
        """
        pass

    def success_hook(self):
        """
        Hook called during cleanup for a successful job.
        """
        return []

    def failure_hook(self):
        """
        Hook called during cleanup.
        """
        return []

    def __repr__(self):
        return self.name
