# Standard Python libraries
import logging
import threading
import time

# Package libraries
from jackhammer.worker import Worker, WorkerStatus
from jackhammer.job import Job, JobState, JobStatus
from jackhammer.jobQueue import JobQueue

class Scheduler(threading.Thread):
    """
    Scheduler to launch and manage workers.

    Uses very basic scheduling behaviour with limited understanding
    of dependencies. Complicated job structures may bring it to
    failure.
    """

    def __init__(self, create_provider, config):
        threading.Thread.__init__(self)
        self.name = "jackhammer.scheduler"

        # Threading
        self.shutdown_flag = threading.Event()
        self.pending = JobQueue()
        self.ready = JobQueue()
        self.completed = JobQueue()

        # State
        self.workers = []
        self.create_provider = create_provider
        self.config = config
        self.failureRate = 0

    def add_job(self, job):
        """
        Add a new job to the pending queue.
        The job must have a corresponding pending state.
        """
        logger.debug("Adding job: %s", job)
        assert job.state == JobState.Pending, "Attempt to add non-pending job"
        self.pending.enqueue(job)

    def shutdown(self):
        """
        Stop the scheduler, cancelling any ongoing jobs.
        Any jobs that did not finish will have their failure
        hooks called, with a SHUTDOWN response.
        """
        logger.info("Scheduler Shutdown: %s", self)
        self.shutdown_flag.set()

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
            self.shutdown()

        for worker in self.workers:
            worker.join(timeout=self.config['joinTimeout'])
            if worker.is_alive():
                logger.error("Failed to join worker: %s", worker)
        self.create_provider().cleanup_machines()

    def scheduler_loop(self):
        """
        The scheduler loop, which manages job and worker life
        cycles.
        """
        while not self.shutdown_flag.is_set():
            # Check for completed jobs, adding new jobs as pending
            for job in self.completed.iter():
                if job.status == JobStatus.Unknown:
                    self.shutdown()
                self.add_jobs(job.cleanup())

            # Promote pending jobs to ready
            pending = []
            for job in self.pending.iter():
                pending.extend(self.prepare_job(job))
            self.add_jobs(pending)

            # Check for any finished workers
            for worker in self.workers:
                if not worker.is_alive():
                    worker.join()
                    self.cleanup_worker(worker)

            # Launch a new worker if not at limit
            if len(self.workers) < self.config['maxWorkers']:
                job = self.ready.dequeue()
                if job != None:
                    self.prepare_worker(job)

            # Deadlock check
            assert len(self.workers) > 0 or self.ready.empty()

            # Rate limiting
            time.sleep(self.config['loopDelay'])

    # Worker Life Cycle
    def prepare_worker(self, job):
        """
        Prepare and start a worker, with an initial job.
        """
        r = Worker(job,
                self.worker_cycle_jobs,
                self.create_provider(),
                self.shutdown_flag)
        self.workers.append(r)
        r.start()

    def worker_cycle_jobs(self, job):
        """
        Callback for workers to return a job and get a one job.

        TODO: The delay here costs money and results in inconsistent
              behaviour. Find a better way.
              Best thing to do is likely to process the completed job here
              and consider any of its children.
        """
        self.completed.enqueue(job)
        if self.shutdown_flag.is_set():
            job = None
        else:
            job = self.ready.dequeue(timeout=self.config['readyTimeout'])

        self.logger.debug("Allocating job: %s", job)
        return job

    def cleanup_worker(self, worker):
        """
        Handle worker termination, checking for infrastructure failures
        and shutting down the scheduler if sufficiently troublesome.
        """
        self.workers.remove(worker)
        if worker.status == WorkerStatus.Success:
            assert worker.job == None, "Worker with job claiming success"
        elif worker.status == WorkerStatus.Disconnected:
            self.failureRate += 1
            self.logger.warning("Worker failure %d: %s", self.failureRate, worker.msg)
            if worker.job:
                worker.job.set_status(JobStatus.Disconnected)
                self.completed.enqueue(worker.job)
        else:
            self.logger.error("Worker shutdown: %s", worker.msg)
            self.shutdown()

    # Job Life Cycle
    def prepare_job(self, job):
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
            return []
        elif job.state == JobState.Completed:
            return job.cleanup()
