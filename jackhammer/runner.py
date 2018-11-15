# Standard Python libraries
import logging
import threading
import time
import uuid
from enum import Enum

# SSH library
import paramiko

# Enum of stages in the runner
class RunnerStage(Enum):
    Create  = "Create"
    Connect = "Connect"
    Command = "Command"
    Unknown = "Unknown"

# Custom excetion class
class RunnerException(Exception):
    def __init__(self, message, stage):
        super().__init__(message)
        self.stage = stage

    def repeat(self):
        return self.stage in [RunnerStage.Create, RunnerStage.Connect]

    def __str__(self):
        return "Runner Failure in %s: %s" % (self.state, super())

# Helper function
def connection(ip, username, pkey, maxIter=5, delay=10):
    i = 0
    while i < maxIter:
        try:
            client = paramiko.client.SSHClient()
            client.set_missing_host_key_policy(paramiko.client.AutoAddPolicy())
            client.connect(ip, username=username, pkey=pkey)
            return client
        except Exception as e:
            time.sleep(delay)
            i += 1
    raise RunnerException("Unable to open SSH connection", RunnerStage.Connect)

class Runner(threading.Thread):

    def __init__(self, job, provider):
        threading.Thread.__init__(self)
        self.logger = logging.getLogger("jackhammer.runner")

        # Fields
        self.job = job
        self.provider = provider
        self.config = job.config
        self.name = job.name.split(':')[0][:10] + '-' + str(uuid.uuid4())[:16]
        self.failure = None

        # SSH configuration
        self.privateKey = paramiko.rsakey.RSAKey.generate(bits=self.config.connection['bits'])
        self.publicKey = self.config.connection['username'] + ':ssh-rsa '
        self.publicKey += self.privateKey.get_base64()
        paramiko.hostkeys.HostKeys().clear()

    def run(self):
        self.logger.info('Starting')
        try:
            self.with_machine_ip()
        except RunnerException as e:
            raise e
        except Exception as e:
            self.set_failure(RunnerStage.Unknown, str(e))
        self.logger.info('Finished')

    def with_machine_ip(self):
        self.logger.info('Creating machine')

        # Create the machine
        try:
            machineName = self.provider.create_machine(self.publicKey, self.name)
            ip = self.provider.get_ip(machineName)
        except Exception as e:
            self.set_failure(RunnerStage.Create, str(e))
            return

        # Find the machine's IP and start the task
        try:
            self.logger.debug('Machine created: %s at %s', machineName, ip)
            self.with_connection(ip)
        finally:
            self.logger.debug('Deleting machine: %s at %s', machineName, ip)
            self.provider.delete_machine(machineName)

    def with_connection(self, ip):
        self.logger.info("Connecting: %s", ip)

        # Open a SSH connection
        try:
            client = connection(ip, self.config.connection['username'], self.privateKey)
        except Exception as e:
            self.set_failure(RunnerStage.Connect, str(e))
            return

        # Run the task, closing the SSH connection after
        try:
            self.job.launch(client)
        except Exception as e:
            self.set_failure(RunnerStage.Command, str(e))
        finally:
            self.logger.info("Disconnecting: %s", ip)
            client.close()

    def set_failure(self, failure, msg):
        self.logger.error("Failed: %s %s", failure, msg)
        self.failure = RunnerException(msg, failure)

    def failed(self):
        return self.failure != None

    def __repr__(self):
        return self.name
