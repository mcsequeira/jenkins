import logging
#=======================================================================================
# Create Logger
#=======================================================================================
logger_name = ""
log = logging.getLogger() #"airflow.task")
log.setLevel(logging.DEBUG)
## create console handler and set level to debug
#ch = logging.StreamHandler()
#ch.setLevel(logging.INFO)
## create formatter
#formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
## add formatter to ch
#ch.setFormatter(formatter)
## add ch to logger
#log.addHandler(ch)
#=======================================================================================================
import google.auth
import google.auth.transport.requests

from datetime import datetime, timedelta
import time
from requests.models import Response
import requests
import json
import urllib3.exceptions
from urllib3.exceptions import ReadTimeoutError

global response
global http_tries
global http_timeout
global http_poll_wait_time
global http_log_wait_time
global response
import inspect
from retrying import retry

response                    = requests.Response()
http_tries                  = 3
http_timeout                = 300
http_poll_wait_time         = 20
http_log_wait_time          = 20
response                    = requests.Response()


#---------------------------------------------------------------------------------------
# GCS Utilities
#---------------------------------------------------------------------------------------
# function that provide the authentication on gcs
#---------------------------------------------------------------------------------------
