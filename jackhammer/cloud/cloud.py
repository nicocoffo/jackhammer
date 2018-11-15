import uuid

from jackhammer.cloud.machine import Machine

class Cloud:
    def __init__(self, config):
        self.config = config
        self.uuid = str(uuid.uuid4())

    def thread_init(self):
        """
        Drivers might not be thread safe.
        """
        pass

    def create_client(self, name):
        """
        Create a machine, returning a paramiko client wrapped in
        class to manage session and machine shutdown.
        """
        name = name[:31] + "-" + self.uuid
        return Machine(name, self.create_machine, self.config['session'])

    def cleanup_machines(self):
        """
        Destroy all used machines.
        """
        for m in self.list_machines():
            m.destroy()

    def list_machines(self):
        """
        List all used machines.
        """
        raise NotImplementedError

    def create_machine(self, name, key):
        """
        Create a new machine resources.
        """
        raise NotImplementedError
