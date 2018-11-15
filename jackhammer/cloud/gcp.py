from jackhammer.cloud.cloud import Cloud

# libcloud
from libcloud.compute.types import Provider
from libcloud.compute.providers import get_driver

class GCP(Cloud):
    def __init__(self, config):
        super().__init__(config)
        self.compute = get_driver(Provider.GCE)

    def thread_init(self):
        self.driver = self.compute(
                self.config['email'],
                self.config['keyPath'],
                project=self.config['project'])

    def create_machine(self, name, key):
        node = self.config['node']
        metadata = node['metadata'] if 'metadata' in node else {}
        metadata['ssh-keys'] = key

        return self.driver.create_node(
                name,
                node['size'],
                node['image'],
                node['zone'],
                ex_tags = node['tags'],
                ex_metadata = metadata,
                ex_preemptible = True)

    def list_machines(self):
        return [m for m in self.driver.list_nodes() if self.uuid in m.name]
