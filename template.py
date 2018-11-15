#!/usr/bin/env python3

import signal
import subprocess
import sys

def signal_handler(sig, frame):
    print('PREEMPTED')
    sys.exit(2)

if __name__ == "main":
    # Setup signal handler
    signal.signal(signal.SIGINT, signal_handler)

    # Launch script, redirect output
    subprocess.run(sys.argv, capture_output=True)

    # Track progress

    # Capture error codes

    # Return either 

