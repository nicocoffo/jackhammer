# Standard Python libraries
import json
import logging
import queue

class JobQueue:

    def __init__(self):
        self.jobs = queue.Queue()

    def empty(self):
        return self.jobs.empty()

    def enqueue(self, job):
        self.jobs.put(job)

    def dequeue(self):
        return self.jobs.get_nowait()

    def mark_done(self, job):
        pass

MAX_FAILURES = 3

class Job:

    def __init__(self, post_bytes, index):
        """
        Create a job from POST bytes and a 'unique' index
        """
        self.logger = logging.getLogger("jackhammer.job")
        self.name = 'job-%d' % index
        self.username = "root"
        self.bits = 2048
        self.scripts = []

        # State
        self.failures = 0
        self.returnCode = None

    def execute(self, client):
        """
        Run the job on the client 
        """
        for script in self.scripts:
            status = script.execute(client)
            if status != 0:
                self.returnCode = status
                return
        self.returnCode = 0
        return

    def completed(self):
        """
        Test if the script completed successfully
        """
        return self.returnCode == 0

    def infra_cleanup(self):
        """
        Cleanup after an infrastructure failure.
        Execution may or may not have started
        """
        pass

    def failure_cleanup(self):
        """
        Cleanup after an execution failure.
        """
        pass

    def success_cleanup(self):
        """
        Cleanup after a successful execution.
        """
        pass

    def repeat(self):
        """
        In the event of a failure, determine whether a repeat is worthwhile.
        """
        # Assume a lack of return code indicates preemption
        if self.returnCode == -1:
            return True

        # Limited number of attempts
        if self.failures < MAX_FAILURES:
            self.failures += 1
            return True

        return False

    def __repr__(self):
        return self.name

    def __str__(self):
        return self.name
