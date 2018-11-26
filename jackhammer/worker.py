# Standard Python libraries
from enum import Enum
from threading import Thread
from time import time
from uuid import uuid4
from logging import getLogger

from jackhammer.job import JobState

logger = getLogger("jackhammer")

class Worker(Thread):
    """
    Worker corresponding to a remote machine.

    Thread should remain alive at least as long as a connection to the
    remote machine is possible or the shutdown_flag is set.

    Will attempt to run several jobs over its lifetime, using the callbacks
    to get and return jobs.

    The thread will measure the duration the machine was available and capture
    any exceptions.
    """

    def __init__(self, job, cycle_job, provider, shutdown_flag):
        Thread.__init__(self)
        self.name = "worker-" + str(uuid4())[:16]
        self.exception = None
        self.duration = None
        self.startTime = None

        # Args
        self.job = job
        self.cycle_job = cycle_job
        self.provider = provider
        self.shutdown_flag = shutdown_flag

    def run(self):
        """
        Thread entry point.
        The worker loop, which opens a machine connection
        and runs through a series of jobs.
        """
        logger.info("Worker Launch: %s", self)
        try:
            self.worker_loop()
        except Exception as e:
            logger.warning("Worker Failure: %s %s", self, str(e))
            self.exception = e

        logger.info("Worker Shutdown: %s", self)
        self.duration = (time() - self.startTime) if self.startTime else None

    def worker_loop(self):
        """
        The worker loop, which requests and executes jobs until
        the remote machine crashes.
        """
        with self.provider.create_client(self.name) as client:
            self.startTime = time()
            while self.job and self.conn_check(client):
                self.job.execute(client, self.shutdown_flag)
                if self.job.state == JostState.Disconnection:
                    break
                self.job = self.cycle_job(self.name, self.job)

    def conn_check(self, client):
        """
        Check the machine is still available by opening a connection
        and running a known command.
        """
        try:
            stdin, stdout, stderr = client.exec_command("echo test", timeout=10)
            l = stdout.readlines()
            return l == ["test\n"]
        except Exception as e:
            return False

    def __repr__(self):
        return self.name
