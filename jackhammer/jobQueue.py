# Standard Python libraries
import json
import logging
import queue
from enum import Enum
import operator

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

    def enqueue_list(self, jobs):
        for j in jobs:
            self.enqueue(j)
