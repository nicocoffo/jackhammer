import uuid

from jackhammer.cloud.machine import Machine


class Cloud:
    """
    Wrapper around libcloud's NodeDriver to allow for the
    creation of machines.
    """

    def __init__(self, uuid, config):
        self.config = config
        self.uuid = uuid

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

        for v in self.list_volumes():
            v.destroy()

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
