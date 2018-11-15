# Standard Python libraries
import logging
import threading
import time

# Package libraries
from jackhammer.worker import Worker, WorkerStatus
from jackhammer.job import Job, JobState
from jackhammer.jobQueue import JobQueue

class Scheduler(threading.Thread):
    """
    Scheduler to launch and manage workers.

    Uses very basic scheduling behaviour with limited understanding
    of dependencies. Complicated job structures may bring it to
    failure.
    """

    def __init__(self, provider, config):
        threading.Thread.__init__(self)
        self.name = "jackhammer.scheduler"
        self.logger = logging.getLogger(self.name)

        # Threading
        self.shutdown_flag = threading.Event()
        self.pending = JobQueue()
        self.ready = JobQueue()
        self.completed = JobQueue()

        # State
        self.workers = []
        self.provider = provider
        self.config = config
        self.failureRate = 0

    def add_job(self, job):
        """
        Simple function to add a new job to the pending queue.
        Its state must be pending.
        """
        assert job.state == JobState.Pending
        self.pending.enqueue(job)

    def add_jobs(self, jobs):
        """
        Wrapper around add_job for a list.
        """
        for j in jobs:
            self.add_job(j)

    def shutdown(self):
        """
        Stop the scheduler, cancelling any ongoing jobs.
        Any jobs that did not finish will have their failure
        hooks called, with a SHUTDOWN response.
        """
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
        self.logger.info("Launching")
        self.provider.thread_init()

        try:
            self.scheduler_loop()
        except Exception as e:
            self.logger.error("Failure in Scheduler loop: %s", str(e))
            self.shutdown()

        for worker in self.workers:
            worker.join(timeout=self.config['joinTimeout'])
            if worker.is_alive():
                self.logger.error("Failed to join worker: %s", worker)
        self.provider.cleanup_machines()
        self.logger.info("Stopping")

    def scheduler_loop(self):
        """
        The scheduler loop, which manages job and worker life
        cycles.
        """
        while not self.shutdown_flag.is_set():
            # Check for completed jobs, adding new jobs as pending
            for job in self.completed.iter():
                self.add_jobs(job.postlaunch())

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

            # Launch new workers if not at limit
            while len(self.workers) < self.config['maxWorkers']:
                job = self.ready.dequeue()
                if job == None:
                    break
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
        r = Worker(job, self.worker_cycle_jobs, self.provider)
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
        elif worker.status == WorkerStatus.Failure:
            self.failureRate += 1
            self.logger.warning("Worker failure %d: %s", self.failureRate, worker.msg)
            if worker.job != None:
                worker.job.reset()
                self.pending.enqueue(worker.job)
        else:
            self.logger.error("Worker shutdown: %s", worker.msg)
            self.shutdown()

    # Job Life Cycle
    def prepare_job(self, job):
        """
        Prepare the job for launch. Calls the prelaunch function to
        determine an initial job state and acts accordingly.

        Returns a list of jobs to place on the pending queue.
        """
        job.prelaunch()
        if job.state == JobState.Pending:
            return [job]
        elif job.state == JobState.Ready:
            job.set_shutdown(self.shutdown_flag)
            self.ready.enqueue(job)
            return []
        elif job.state == JobState.Completed:
            return job.postlaunch()
        assert False, "Invalid post-prelaunch state: %s" % job.state
