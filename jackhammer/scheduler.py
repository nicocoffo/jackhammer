# Standard Python libraries
from threading import Thread, Event
from time import sleep
from logging import getLogger
import traceback

# Package libraries
from jackhammer.worker import Worker
from jackhammer.job import Job, JobState
from jackhammer.jobQueue import JobQueue

logger = getLogger("jackhammer")


class Scheduler(Thread):
    """
    Scheduler to launch and manage workers.

    Uses very basic scheduling behaviour with limited understanding
    of dependencies. Complicated job structures may bring it to
    failure.

    TODO: Consider moving job cleanup to worker thread callback.
    """

    def __init__(self, create_provider, config):
        Thread.__init__(self)
        self.name = "jackhammer.scheduler"
        self.exception = None

        # Threading
        self.shutdownFlag = Event()
        self.pending = JobQueue()
        self.ready = JobQueue()
        self.completed = JobQueue()

        # State
        self.workers = []
        self.create_provider = create_provider
        self.config = config
        self.failureRate = 0

    def add_job(self, job, quiet=False):
        """
        Add a new job to the pending queue.
        The job must have a corresponding pending state.
        """
        if not quiet:
            logger.debug("Adding job: %s", job)
        assert job.state == JobState.Pending, "Attempt to add non-pending job"
        self.pending.enqueue(job)

    def shutdown(self):
        """
        Stop the scheduler, cancelling any ongoing jobs.
        """
        logger.debug("Early shutdown: %s", self)
        self.shutdownFlag.set()

    # Internal Functions
    def run(self):
        """
        Thread entry point.
        Run the job scheduler, which will spin up machines
        and ensure they are cleaned up.
        
        In the event of failure, all remaining workers are given
        some time to clean up, with a forced clean up through the
        machine provider.
        """
        logger.info("Scheduler Launch: %s", self)
        try:
            self.scheduler_loop()
        except Exception as e:
            logger.error("Scheduler Failure: %s", str(e))
            logger.error(traceback.format_exc())
            self.exception = e

        logger.info("Scheduler Shutdown: %s", self)
        self.shutdownFlag.set()
        for worker in self.workers:
            try:
                self.worker_cleanup(worker, timeout=self.config['joinTimeout'])
            except Exception as e:
                logger.error("Worker Cleanup Failure: %s", str(e))
                logger.error(traceback.format_exc())
        self.create_provider().cleanup_machines()

    def scheduler_loop(self):
        """
        The scheduler loop, which manages job and worker life
        cycles.
        """
        while not self.shutdownFlag.is_set():
            # Check for completed jobs, adding new jobs as pending
            for job in self.completed.iter():
                self.job_cleanup(job)

            # Promote pending jobs to ready
            pending = []
            for job in self.pending.iter():
                pending.extend(self.job_prepare(job))
            for job in pending:
                self.add_job(job, True)

            # Check for any finished workers
            for worker in self.workers:
                if not worker.is_alive():
                    self.worker_cleanup(worker)

            # Launch a new worker if not at limit
            if not self.ready.empty():
                if len(self.workers) < self.config['maxWorkers']:
                    self.worker_launch()

            # Deadlock check
            assert len(self.workers) > 0 or self.pending.empty()

            # Rate limiting
            sleep(self.config['loopDelay'])

    # Job Functions
    def job_prepare(self, job):
        """
        Prepare the job for launch. Calls the prepare function to
        determine an initial job state and acts accordingly.

        Returns a list of jobs to place on the pending queue.
        """
        job.prepare()
        if job.state == JobState.Pending:
            return [job]
        elif job.state == JobState.Ready:
            self.ready.enqueue(job)
        else:
            self.completed.enqueue(job)
        return []

    def job_cleanup(self, job):
        """
        Handle job termination. Ensures the job ended with a valid state.
        """
        if job.exception:
            logger.error("Job %s failed, raising exception", job)
            raise job.exception
        for j in job.cleanup():
            self.add_job(j)

    # Worker Functions
    def worker_launch(self):
        """
        Launch a worker, with all necessary callbacks.
        """
        r = Worker(self.worker_get_job, self.worker_return_job,
                   self.create_provider(), self.shutdownFlag)
        self.workers.append(r)
        r.start()

    def worker_cleanup(self, worker, timeout=None):
        """
        Handle worker termination, checking for infrastructure failures
        and shutting down the scheduler if sufficiently troublesome.
        """
        worker.join(timeout=timeout)
        if worker.is_alive():
            raise Exception("Failed to join worker: %s" % worker)
        self.workers.remove(worker)

        if worker.exception:
            logger.error("%s failed, raising exception", worker)
            raise worker.exception
        elif worker.duration < self.config['minWorkerDuration']:
            logger.warning("%s had a short duration: %f", worker, worker.dur)
            self.failureRate += 1

    def worker_get_job(self, worker):
        """
        Callback for a worker to request a job.
        """
        job = self.ready.dequeue(timeout=self.config['readyTimeout'])
        logger.debug("Giving %s: %s", worker, job)
        return job

    def worker_return_job(self, worker, job):
        """
        Callback for a worker to return a job.
        """
        logger.debug("%s returned: %s", worker, job)
        self.completed.enqueue(job)

    def __repr__(self):
        return self.name
