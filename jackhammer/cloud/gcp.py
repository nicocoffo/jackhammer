import logging

from jackhammer.cloud.cloud import Cloud

# libcloud
from libcloud.compute.types import Provider
from libcloud.compute.providers import get_driver

logger = logging.getLogger("jackhammer.cloud")


class GCP(Cloud):
    """
    Wrapper around libcloud's GCP NodeDriver. Creates preemptible
    machines, to keep costs low.
    """

    def __init__(self, config):
        super().__init__(config)
        logger.debug("Creating GCP node driver")
        self.compute = get_driver(Provider.GCE)
        self.driver = self.compute(
            self.config['email'],
            self.config['keyPath'],
            project=self.config['project'])

    def create_machine(self, name, key):
        logger.debug("Creating GCP node")
        node = self.config['node']
        metadata = node['metadata'] if 'metadata' in node else {}
        metadata['ssh-keys'] = key

        return self.driver.create_node(
            name,
            node['size'],
            node['image'],
            node['zone'],
            ex_tags=node['tags'],
            ex_metadata=metadata,
            ex_preemptible=True)

    def list_machines(self):
        return [m for m in self.driver.list_nodes() if self.uuid in m.name]
