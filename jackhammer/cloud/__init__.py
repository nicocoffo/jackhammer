from jackhammer.cloud.gcp import GCP
from jackhammer.cloud.machine import CloudCreate

class CloudPreempt(Exception):
    def __init__(self):
        super().__init__("Cloud machine pre-empted")
