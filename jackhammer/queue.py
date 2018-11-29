# Standard Python libraries
from queue import PriorityQueue, Empty


class JobQueue:
    """
    Wrapper around python queue, mostly focused
    on simplyfing use of the dequeue operation.
    """

    def __init__(self):
        self.jobs = PriorityQueue()

    def empty(self):
        return self.jobs.empty()

    def enqueue(self, job):
        self.jobs.put(job)

    def dequeue(self, timeout=None):
        try:
            return self.jobs.get(timeout != None, timeout)
        except Empty:
            return None

    def iter(self):
        return iter(self.dequeue, None)

    def __len__(self):
        return self.jobs.qsize()
