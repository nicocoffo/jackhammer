# Standard Python libraries
import logging
import threading
import time
import uuid
from enum import Enum

from jackhammer.cloud import CloudCreate

class WorkerStatus(Enum):
    Success  = 0
    Failure  = 1
    Shutdown = 2

class Disconnection(Exception):
    def __init__(self):
        super().__init__("Machine disconnected")

class Worker(threading.Thread):
    """
    Worker corresponding to a remote machine.
    May run several jobs over its life time.
    """
    DONE_MSG = "No remaining jobs, scaling down"
    PRE_MSG = "Machine was pre-empted"
    CREATE_MSG = "Unable to create machine"
    ERR_MSG = "Exception in worker loop: %s"

    def __init__(self, job, cycle_jobs, provider, shutdown):
        threading.Thread.__init__(self)
        self.name = "worker-" + str(uuid.uuid4())[:16]
        self.logger = logging.getLogger(self.name)

        # Args
        self.job = job
        self.cycle_jobs = cycle_jobs
        self.provider = provider
        self.shutdown = shutdown

        # State
        self.status = None
        self.msg = ""

    def run(self):
        """
        Thread entry point.
        The worker loop, which opens a machine connection
        and runs through a series of jobs.
        """
        self.logger.info("Launching")
        self.provider = self.provider.thread_init()

        try:
            self.worker_loop()
        except Disconnection:
            self.state = WorkerStatus.Failure
            self.msg = self.PRE_MSG
        except CloudCreate:
            self.status = WorkerStatus.Failure
            self.msg = self.CREATE_MSG
        except Exception as e:
            self.status = WorkerStatus.Shutdown
            self.msg = self.ERR_MSG % str(e)

        self.logger.info("Stopping")

    def worker_loop(self):
        """
        The worker loop, which requests and executes jobs until
        the remote machine crashes.
        """
        with self.provider.create_client(self.name) as client:
            while self.job != None and self.check_machine(client):
                self.job.execute(client, self.shutdown)
                self.job = self.cycle_jobs(self.job)
        self.status = WorkerStatus.Success
        self.msg = self.DONE_MSG

    def check_machine(self, client):
        """
        Check the machine is still available.
        """
        try:
            stdin, stdout, stderr = client.exec_command("echo test")
            out = stdout.readlines()
        except Exception as e:
            out = None
        if out != ["test\n"]:
            raise Disconnection
        return True
