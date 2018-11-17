from time import sleep
from paramiko.rsakey import RSAKey
from paramiko.hostkeys import HostKeys
from paramiko.client import AutoAddPolicy, SSHClient


class Machine:
    """
    Abstraction over a machine and SSH connection, for use with
    Python's with syntax.
    """

    def __init__(self, name, create_machine, config):
        # Args
        self.name = name
        self.create_machine = create_machine
        self.username = config['username']
        self.config = config

        # Resources
        self.machine = None
        self.client = None

        # SSH key
        self.private = RSAKey.generate(bits=config['keyBits'])
        self.public = self.username + ':ssh-rsa ' + self.private.get_base64()
        HostKeys().clear()

    def create_client(self, ip):
        c = SSHClient()
        c.set_missing_host_key_policy(AutoAddPolicy())
        c.connect(ip, username=self.username, pkey=self.private)
        self.client = c
        return self.client

    def __enter__(self):
        exception = None
        try:
            # Create the machine
            self.machine = self.create_machine(self.name, self.public)
            ip = self.machine.public_ips[0]

            # Wait for a connection
            for i in range(10):
                try:
                    return self.create_client(ip)
                except Exception as e:
                    exception = e
                sleep(2)
        except Exception as e:
            exception = e

        # Cleanup if connection failed
        if self.machine:
            self.machine.destroy()
        raise exception

    def __exit__(self, exc_type, exc_value, tb):
        try:
            if self.client:
                self.client.close()
        finally:
            if self.machine:
                self.machine.destroy()
