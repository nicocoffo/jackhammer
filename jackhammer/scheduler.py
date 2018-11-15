# Standard Python libraries
import logging
import threading
import time

# Package libraries
from .runner import Runner
from .job import Job, JobState
from .jobQueue import JobQueue

class Scheduler(threading.Thread):
    """
    Scheduler to launch and manage runners.

    Uses very basic scheduling behaviour with limited understanding
    of dependencies. Complicated job structures may bring it to
    failure.
    """
    name = "jackhammer.scheduler"
    maxInfraFailures = 3

    def __init__(self,
                 provider,
                 queue=JobQueue(),
                 pollDelay=2,
                 cleanupTimeout=240,
                 maxRunners=5):
        threading.Thread.__init__(self)
        self.logger = logging.getLogger(self.name)

        # Threading
        self.shutdown_flag = threading.Event()
        self.jobs = queue

        # Config
        self.provider = provider
        self.pollDelay = pollDelay
        self.cleanupTimeout = cleanupTimeout
        self.maxRunners = maxRunners

        # State
        self.runners = []
        self.infraFailures = 0

    def add_job(self, job):
        """
        Simple function to wrap accesses to the job queue.
        """
        self.jobs.enqueue(job)

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
        Run the job scheduler, which will spin up machines
        and ensure they are cleaned up.

        Thread entry point.
        """
        try:
            self.logger.debug("Launching " + self.name)
            self.scheduler_loop()
        except Exception as e:
            self.logger.error("Failure in scheduler loop: " + str(e))
            self.shutdown()
        finally:
            # Try to join all remaining threads
            for runner in self.runners:
                runner.join(timeout=self.cleanupTimeout)

            # Cleanup any left over machines
            self.provider.create().delete_all_machines()

    def launch_job(self, job):
        """
        Launch a runner with the provided job.
        """
        job.set_shutdown(self.shutdown_flag)
        r = Runner(job, self.provider.create())
        self.runners.append(r)
        r.start()

    def cleanup_job(self, job):
        """
        Clean up a job, with logic to manage different results.

        Runs the jobs postlaunch behaviour and schedules any new
        jobs. This includes rescheduling any failures.
        """
        # Add any new jobs
        newJobs = job.postlaunch()
        self.jobs.enqueue_list(newJobs)
        for j in newJobs:
            j.state = JobState.Queued

        # Log the result of the job
        if job.state == JobState.Completed:
            self.logger.debug("Finishing job: %s" % job)
            self.jobs.mark_done(job)
        elif job.state == JobState.Failed:
            if job in newJobs:
                self.logger.error("Reattempting job: %s" % job)
            else:
                self.logger.error("Failing job: %s" % job)
            self.jobs.mark_done(job)
            
    def infra_failure(self, runner):
        """
        Handle infrastructure failures, encoutered by the runner.

        Certain failures are ignored, to a limit. Others are
        considered terminal, as they will likely fail multiple
        jobs.
        """
        self.logger.error("Infrastructure failure: %s" % runner)

        if runner.failure.repeat() and self.infraFailures < self.maxInfraFailures:
            self.infraFailures += 1
            self.logger.debug("Restarting job %s" % runner.job)
            self.launch_job(runner.job)
        else:
            self.logger.error("Too many infrastructure failures, shutting down")
            self.shutdown()

    def scheduler_loop(self):
        """
        The scheduler loop, which pulls and launches pending jobs.
        """
        while not self.shutdown_flag.is_set():
            # Grab a job and launch if desired
            popped = []
            while not self.jobs.empty() and len(self.runners) < self.maxRunners:
                job = self.jobs.dequeue()
                job.prelaunch()
                if job.state == JobState.Queued:
                    popped.append(job)
                elif job.state == JobState.Executing:
                    self.launch_job(job)
                    break
                else:
                    self.cleanup_job(job)
            self.jobs.enqueue_list(popped)

            # Manage running jobs
            for runner in self.runners:
                if not runner.is_alive():
                    runner.join()
                    self.runners.remove(runner)
                    if not runner.failed():
                        self.cleanup_job(runner.job)
                    else:
                        self.infra_failure(runner)

            # Delay
            time.sleep(self.pollDelay)
