# Standard Python libraries
import logging
import threading
import time
import json

# SSH library
import paramiko

# Failure Definitions
FAILURE_CREATE = "FAILURE-CREATE"
FAILURE_CONNECT = "FAILURE-CONNECT"
FAILURE_SEND = "FAILURE-SEND"
FAILURE_CMD = "FAILURE-CMD"
FAILURE_UNKNOWN = "FAILURE-UNKNOWN"

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
    raise Exception


class Runner(threading.Thread):

    def __init__(self, job, gcp):
        threading.Thread.__init__(self)
        self.logs = ""
        self.logger = logging.getLogger("jackhammer.runner")

        self.job = job
        self.gcp = gcp
        self.name = self.job.name

        self.failure = None
        self.privateKey = paramiko.rsakey.RSAKey.generate(bits=self.job.bits)
        self.publicKey = self.job.username + ':ssh-rsa '
        self.publicKey += self.privateKey.get_base64()
        paramiko.hostkeys.HostKeys().clear()

    def run(self):
        self.logger.info('Starting')
        try:
            self.with_machine_ip()
        except Exception as e:
            self.set_failure(FAILURE_UNKNOWN, str(e))
        self.logger.info('Finished')

    def with_machine_ip(self):
        self.logger.info('Creating machine')

        # Create the machine
        try:
            machineName = self.gcp.create_machine(self.publicKey, self.name)
            ip = self.gcp.get_ip(machineName)
        except Exception as e:
            self.set_failure(FAILURE_CREATE, str(e))
            return

        # Find the machine's IP and start the task
        try:
            self.logger.info('Machine created: %s at %s', machineName, ip)
            self.with_connection(ip)
        finally:
            self.logger.info('Deleting machine: %s at %s', machineName, ip)
            self.gcp.delete_machine(machineName)

    def with_connection(self, ip):
        self.logger.info("Connecting: %s", ip)

        # Open a SSH connection
        try:
            client = connection(ip, self.job.username, self.privateKey)
        except Exception as e:
            self.set_failure(FAILURE_CONNECT, str(e))
            return

        # Run the task, closing the SSH connection after
        try:
            self.job.execute(client)
        except Exception as e:
            self.set_failure(FAILURE_CMD, str(e))
        finally:
            self.logger.info("Disconnecting: %s", ip)
            client.close()

    def set_failure(self, failure, msg):
        self.logger.error("Failed: %s %s", failure, msg)
        self.failure = failure

    def failed(self):
        return self.failure != None

    def repeat(self):
        return self.failure in [FAILURE_CREATE, FAILURE_CONNECT, FAILURE_SEND]

    def __repr__(self):
        return self.name
