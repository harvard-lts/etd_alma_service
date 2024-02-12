import os
import sys
import time
from pathlib import Path
import requests
from requests.packages import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
# heartbeat script for docker/k8s

hbeat_path = os.getenv("HEARTBEAT_FILE", "/tmp/worker_heartbeat")
HEARTBEAT_FILE = Path(hbeat_path)
HEARTBEAT_WINDOW = int(os.getenv("HEARTBEAT_WINDOW", 60))
ALMA_API_KEY = os.getenv("ALMA_API_KEY")
ALMA_HEALTHCHECK_URL = os.getenv("ALMA_HEALTHCHECK_URL") + ALMA_API_KEY
JOBMON_URL = os.getenv("jobMonitor")
INSTANCE = os.getenv("INSTANCE", "dev")

# check alma
try:
    # need verify false b/c using selfsigned certs
    r = requests.get(ALMA_HEALTHCHECK_URL, verify=False)
    if (r.status_code != 200):
        print("alma healthcheck failed")
        sys.exit(1)
except Exception:
    print("alma healthcheck failed")
    sys.exit(1)

# check jobmon if prod
if INSTANCE == "prod":
    if JOBMON_URL is None:
        print("jobmon url not set")
        sys.exit(1)
    try:
        r = requests.get(JOBMON_URL, verify=False)
        if (r.status_code != 200):
            print("jobmon healthcheck failed")
            sys.exit(1)
    except Exception:
        print("jobmon healthcheck failed")
        sys.exit(1)

# check timestamp file
try:
    current_ts = int(time.time())
    fstats = os.stat(HEARTBEAT_FILE)
    mtime = int(fstats.st_mtime)
    seconds_diff = int(current_ts - mtime)

    if (seconds_diff < HEARTBEAT_WINDOW):
        print("healthy: last updated %d secs ago" % (seconds_diff))
        sys.exit(0)
    else:
        print("UNHEALTHY: last updated %d secs ago" % (seconds_diff))
        sys.exit(1)

except Exception:
    print("Error: file not found")
    sys.exit(1)
