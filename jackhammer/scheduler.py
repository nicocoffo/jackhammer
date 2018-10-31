# Standard Python libraries
from http.server import BaseHTTPRequestHandler, HTTPServer
import logging
import threading
import time

# Package libraries
from .runner import Runner
from .job import Job, JobQueue

MAX_INDEX = 99999

class Scheduler(threading.Thread):
    """
    Launches the server and runner threads.
    """
    name = "jackhammer.scheduler"

    def __init__(self,
                 provider,
                 job_class=Job,
                 queue=JobQueue(),
                 port=9321,
                 pollDelay=0.5,
                 cleanupTimeout=10):
        threading.Thread.__init__(self)
        self.logger = logging.getLogger(self.name)

        # Threading
        self.shutdown_flag = threading.Event()
        self.jobs = queue

        # Config
        self.provider = provider
        self.job_class = job_class
        self.port = port
        self.pollDelay = pollDelay
        self.cleanupTimeout = cleanupTimeout

        # State
        self.runners = []
        self.infraFailures = 0
        self.index = 0

        # Server
        self.server = self.create_job_server() if self.port != None else None

    def run(self):
        """
        Run the job scheduler.
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
                runner.job.failure_cleanup()

            # Cleanup any left over machines
            self.provider.delete_all_machines()

    def shutdown(self):
        """
        Stop the machine banger, cancelling any ongoing jobs
        """
        self.shutdown_flag.set()
        threading.Thread(target=self.server.shutdown, daemon=True).start()

    def launch_job(self, job):
        """
        Launch a runner with the provided job.
        """
        r = Runner(job, self.provider)
        self.runners.append(r)
        r.start()

    def create_job(self, post):
        try:
            job = self.job_class(post, self.index)
            self.index += 1
            if self.index > MAX_INDEX:
                self.index = 0
            return job
        except Exception as e:
            self.logger.error("Failed to create job: " + str(e))
            return None

    def cleanup_job(self, job):
        """
        Clean up a job, with logic to manage different results.
        """
        if job.completed():
            self.logger.info("Finishing job: %s" % job)
            self.jobs.mark_done(job)
            job.success_cleanup()
        elif job.repeat():
            self.logger.info("Restarting job %s" % job)
            self.launch_job(job)
        else:
            self.logger.error("Giving up on job: %s" % job)
            job.failure_cleanup()
            self.shutdown()

    def infra_failure(self, runner):
        """
        Handle infrastructure failures, encoutered by the runner.
        """
        self.logger.error("Infrastructure failure: %s" % runner)

        # TODO: Better measure of infra failures (rate? region?)
        if runner.repeat() and self.infraFailures < self.maxInfraFailures:
            self.infraFailures += 1
            self.logger.info("Restarting job %s" % runner.job)
            self.launch_job(runner.job)
        else:
            self.logger.error("Too many infrastructure failures, shutting down")
            runner.job.infra_cleanup()
            self.shutdown()

    def scheduler_loop(self):
        """
        The scheduler loop, which pulls and launches pending jobs.
        """
        while not self.shutdown_flag.is_set():
            # Grab a job and launch if desired
            while not self.jobs.empty():
                job = self.create_job(self.jobs.dequeue())
                if job:
                    self.launch_job(job)
                    break

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

    def create_job_server(self):
        """
        Create a server to receive new jobs, as JSON POSTs.
        Also implements a basic health check via GET.
        """
        queue = self.jobs

        class JobServer(BaseHTTPRequestHandler):

            def _set_headers(self):
                self.send_response(200)
                self.send_header('Content-type', 'text/html')
                self.end_headers()

            def do_POST(self):
                """
                Recieve new jobs as POST
                """
                content_length = int(self.headers['Content-Length'])
                post_data = self.rfile.read(content_length)
                queue.enqueue(post_data)
                self._set_headers()

            def do_GET(self):
                """
                Health checks over GET
                """
                self._set_headers()
                return bytes("", 'UTF-8')

        server = HTTPServer(('', self.port), JobServer)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        return server
