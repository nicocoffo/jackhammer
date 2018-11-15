import paramiko
import time

class CloudCreate(Exception):
    def __init__(self):
        super().__init__("Cannot create cloud machine")

class Machine:
    def __init__(self, name, create, config):
        # Args
        self.name = name
        self.create = create
        self.config = config

        # Resources
        self.machine = None
        self.client = None

        # SSH key
        self.private = paramiko.rsakey.RSAKey.generate(bits=config['keyBits'])
        self.public = config['username'] + ':ssh-rsa '
        self.public += self.private.get_base64()
        paramiko.hostkeys.HostKeys().clear()
        self.create = create

    def __enter__(self):
        # Create the machine
        try:
            self.machine = self.create(self.name, self.public)
            ip = self.machine.public_ips[0]

            # Wait for a connection
            i = 0
            while i < self.config['attempts']:
                try:
                    self.client = paramiko.client.SSHClient()
                    self.client.set_missing_host_key_policy(paramiko.client.AutoAddPolicy())
                    self.client.connect(ip, username=self.config['username'], pkey=self.private)
                    return self.client
                except Exception as e:
                    time.sleep(self.config['delay'])
                    i += 1
        except Exception as e:
            pass
        # Cleanup if connection failed
        if self.machine:
            self.machine.destroy()
        raise CloudCreate

    def __exit__(self, exc_type, exc_value, tb):
        try:
            if self.client:
                self.client.close()
        finally:
            if self.machine:
                self.machine.destroy()
