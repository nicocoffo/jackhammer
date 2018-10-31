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
        self.payload = json.loads(post_bytes.decode('utf-8'))
        self.name = 'task-%d' % index
        self.username = "root"
        self.bits = 2048
        self.machineLogs = b''
        self.scripts = []

        # State
        self.state = None
        self.failure = False
        self.failures = 0

    def set_failure(self, state, msg):
        """
        Manage failure cases.
        """
        self.failure = True
        self.state = state
        self.logger.error(("Job %s: " % self.name) + ("%s %s" % (state, msg)))

    def set_state(self, state):
        self.state = state

    def execute(self, client):
        """
        Run the job on the client 
        """
        for script in self.scripts:
            if not script.execute(client):
                break
        return

    def completed(self):
        """
        Test if the script completed successfully
        """
        return True

    def repeat(self):
        """
        Test if the job should be repeated, in event of failure
        """
        return False

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

    def __repr__(self):
        return self.name

    def __str__(self):
        return self.name

    def repeat(self):
        """
        In the event of a failure, determine whether a repeat is worthwhile.
        """
        # TODO: Better logic
        #if self.failures < MAX_FAILURES:
        #    self.logger.error("Repeating job %s" % self.name)
        #    self.failures += 1
        #    return True
        return False


