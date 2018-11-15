#!/usr/bin/env python3

import os
import time
import threading
import random
import string
import logging

import googleapiclient.discovery

def generate_uid(size=32, chars=string.ascii_lowercase + string.digits):
  return ''.join(random.choice(chars) for _ in range(size))

class GCPGen:
    def __init__(self, project, zone, uid=None):
        self.project = project
        self.zone = zone
        self.uid = generate_uid() if uid == None else uid

    def create(self):
        return GCP(self.project, self.zone, self.uid)

class GCP:
    def __init__(self, project, zone, uid):
        self.uid = generate_uid() if uid == None else uid
        self.compute = googleapiclient.discovery.build('compute', 'v1',
                cache_discovery=False)
        self.project = project
        self.zone = zone

    def create_machine(self, sshkey, name):
        """
        Create a machine with the desired configuration.

        Waits for the creation to finish and returns the
        machine's name.
        """
        name = name + '-' + self.uid
        result = self._create(sshkey, name, True)
        return name

    def delete_machine(self, name):
        """
        Delete a machine with the specified name.

        Wait for the deletion to finish.
        """
        self._delete(name, True)

    def get_ip(self, name):
        """
        Get a machine's IPv4 address.
        """
        result = self.compute.instances().get(project=self.project,
                zone=self.zone, instance=name).execute()
        return result['networkInterfaces'][0]['accessConfigs'][0]['natIP']

    def delete_all_machines(self):
        """
        Clean up any machines created for this conversion process,
        based on the existance of the uid in its name.

        These will be removed in parallel, to improve shutdown
        performance.
        """
        ops = []
        machines = self._list()

        if machines == None or len(machines) < 1:
            return

        logging.debug("Removing %d machines:" % len(machines))
        for machine in machines:
            logging.debug("  Instance %s" % machine['name'])
            ops.append(self._delete(machine['name'])['name'])

        for op in ops:
            self._wait(op)

    def _list(self):
        result = self.compute.instances().list(project=self.project,
                zone=self.zone).execute()
        if 'items' in result:
            return [r for r in result['items'] if self.uid in r['name']]
        return None

    def _create(self, sshkey, name, wait=False):
        image_response = self.compute.images().get(
            project=self.project, image='media-image-2').execute()
        source_disk_image = image_response['selfLink']

        # Configure the machine
        machine_type = "zones/%s/machineTypes/n1-standard-1" % self.zone

        config = {
            'name': name,
            'machineType': machine_type,

            'disks': [
                {
                    'boot': True,
                    'autoDelete': True,
                    'initializeParams': {
                        'sourceImage': source_disk_image,
                    }
                }
            ],

            "scheduling": {
                'preemptible': True
            },

            'tags' : {
                'items' : [
                    'collector'
                ]
            },

            "metadata": {
                "items" : [
                    {
                        "key": "ssh-keys",
                        "value": sshkey
                    }
                ]
            },

            'networkInterfaces': [{
                'network': 'global/networks/default',
                'accessConfigs': [
                    {'type': 'ONE_TO_ONE_NAT', 'name': 'External NAT'}
                ]
            }],
        }

        result = self.compute.instances().insert(
            project=self.project,
            zone=self.zone,
            body=config).execute()

        if wait:
            self._wait(result['name'])

        return result

    def _delete(self, name, wait=False):
        result = self.compute.instances().delete(
            project=self.project,
            zone=self.zone,
            instance=name).execute()

        # Remove from list

        if wait:
            self._wait(result['name'])

        return result

    def _wait(self, operation):
        while True:
            result = self.compute.zoneOperations().get(
                project=self.project,
                zone=self.zone,
                operation=operation).execute()

            if result['status'] == 'DONE':
                if 'error' in result:
                    raise Exception(result['error'])
                return result

            time.sleep(1)
