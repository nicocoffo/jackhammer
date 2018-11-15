# Standard Python libraries
import queue

class JobQueue:
    """
    Wrapper around python queue, mostly focused
    on simplyfing use of the dequeue operation.
    """

    def __init__(self):
        self.jobs = queue.Queue()

    def empty(self):
        return self.jobs.empty()

    def enqueue(self, job):
        self.jobs.put(job)

    def dequeue(self, timeout=None):
        try:
            return self.jobs.get(timeout != None, timeout)
        except queue.Empty:
            return None

    def iter(self):
        return iter(self.dequeue, None)
