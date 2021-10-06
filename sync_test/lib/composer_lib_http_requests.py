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
def get_auth_token():
    log.info("____Function: " + str(inspect.stack()[0][3]) + " - Start")
    creds, projects = google.auth.default()
    # creds.valid is False, and creds.token is None
    if creds.valid == False:
        auth_req = google.auth.transport.requests.Request()
        # Need to refresh credentials to populate those
        creds.refresh(auth_req)
    
    else:
        cr_dt = creds.expiry
        now_dt  = datetime.now()
        elapsed = now_dt - cr_dt
        elps = elapsed.total_seconds()
        if elps < 500: creds.refresh(auth_req)
    
    #auth_req = google.auth.transport.requests.Request()
    #creds.before_request(auth_req, "GET", url_prefix, headers)
    log.info("____Function: " + str(inspect.stack()[0][3]) + " - End")
    return creds.token


#=======================================================================================
# HTTP Requests function
#=======================================================================================
# function to perform http requests
#---------------------------------------------------------------------------------------
def http_call(indent, url, data, js, type, attempt):
    log.info("____Function: " + str(inspect.stack()[0][3]) + " - Start")
    #http_tries = 3
    response            = requests.Response()
    token               = "Bearer " + get_auth_token()
    headers             = {"Authorization": token, "Accept": "application/json"}
    
    log.info("url : " +str(url))
    log.info("Attempt : " + str(attempt + 1))
    response    = requests.Response()
    try:
        if type == "post":
            response    = requests.post(url, headers=headers, data=data, json = js, timeout=http_timeout)
        elif type == "get":
            response    = requests.get(url = url, headers=headers,params = data, timeout=http_timeout)
        elif type == "get_log":
            response    = requests.get(url = url, headers=headers,params = data, timeout=http_timeout)
        else:
            log.error("Type Invalid")
            exit(1)    
    except requests.exceptions.Timeout as tout:
        log.error(str(tout))
        response.status_code    = 1101
        response._content       = "HTTP Read Timeout".encode()
    
    except urllib3.exceptions.ReadTimeoutError  as tout:
        log.error(str(tout))
        response.status_code    = 1101
        response._content       = "HTTP Read Timeout".encode()
    
    except ReadTimeoutError as tout:
        log.error(str(tout))
        response.status_code    = 1101
        response._content       = "HTTP Read Timeout".encode()
    
    except urllib3.exceptions.HTTPError as tout:
        log.error(str(tout))
        response.status_code    = 1101
        response._content       = "HTTP Error".encode()
    
    except Exception as e:
        log.error(str(e))
        response.status_code    = 1101
        response._content       = "HTTP Error".encode()
    
    if response.status_code != 200:
        log.error("Response.status_code     : " + str(response.status_code))
        log.error("Response.text            : " + response.text)
        log.error("Attempt #: " + str(attempt +1) + " / " + str(int(http_tries) + 1) + " - HTTP Call Failed.")
        if attempt < http_tries and type != "post":
            log.info("Waiting " + str(5) + " secs...")
            time.sleep(5)
            response = http_call(indent, url, data, js, type, attempt + 1)
        else:
            if response.status_code == 404 : response.status_code = 1102
        log.info("____Function: " + str(inspect.stack()[0][3]) + " - End")
        return response
    
    if type != "get_log":
        try:
            res_json    = response.json()
        except:
            #res_txt = "Response not parsable as JSON"
            log.error(response.text)
            log.error("Response.status_code     : " + str(response.status_code))
            log.error("Response.text            : " + response.text)
            response.status_code = 1101
            response._content    = "Response not parsable as JSON".encode()
    
    if type == "post" and isinstance(response.json(), list):
        if response.json()[-1]["statusCode"] != 200:
            log.error("Response.status_code     : " + str(response.status_code))
            log.error("Response.text            : " + response.text)
            response.status_code = response.json()[-1]["statusCode"]
            if response.status_code == 404: response.status_code = 1103
            response._content       = response.json()[-1]["error"].encode()
        log.info("____Function: " + str(inspect.stack()[0][3]) + " - End")
        return response
    
    log.info("____Function: " + str(inspect.stack()[0][3]) + " - End")
    return response



#function to retrieve data fusion pipeline instance status
#---------------------------------------------------------------------------------------
def check_runid(indent, url, params, stats, check_status):
    log.info("____Function: " + str(inspect.stack()[0][3]) + " - Start")
    response  = http_call(indent, url, params, "", "get", 0)
    if response.status_code != 200: 
        log.info("____Function: " + str(inspect.stack()[0][3]) + " - End")
        return response, "ERROR"
    
    runs                = response.json()
    
    if stats.run_id == "n/a":
        check_status = runs[-1]["status"].upper()
    else:
        for run in runs:
            #log.info("Run   : " + str(run))
            if run["runid"] == stats.run_id :
                check_status = run["status"].upper()
                break
    
    log.info("____Function: " + str(inspect.stack()[0][3]) + " - End")
    return response, check_status



#function to retrieve data fusion pipeline instance status
#---------------------------------------------------------------------------------------
@retry(stop_max_attempt_number=5, wait_exponential_multiplier=1000, wait_exponential_max=32000)
def df_check_status(indent, url_prefix, pipeline_name,program_type,program_id,stats,label):
    log.info("____Function: " + str(inspect.stack()[0][3]) + " - Start")
    check_status        = "UNKNOWN"
    url                 = "{0}apps/{1}/{2}s/{3}/runs/".format(url_prefix,pipeline_name,program_type,program_id)
    params              = {}
    
    if label == "ack":
        while check_status == "UNKNOWN":
            response, check_status = check_runid(indent, url, params, stats, check_status)
            log.info("Instance non started yet... waiting " + str(http_poll_wait_time) + " secs...")
            time.sleep(http_poll_wait_time)
        
        log.info("Instance Started.")
    
    elif label == "status":
        #if check_status == "RUNNING" or check_status == "STARTING" or check_status == "PENDING":
        while check_status in ("UNKNOWN", "PENDING", "STARTING", "RUNNING"):
            response, check_status = check_runid(indent, url, params, stats, check_status)
            log.info("Instance running... waiting " + str(http_poll_wait_time) + " secs...")
            time.sleep(http_poll_wait_time)
        
        log.info("No Instance running.")
    
    log.info("____Function: " + str(inspect.stack()[0][3]) + " - End")
    return response


#function to 
#---------------------------------------------------------------------------------------
def df_status_recovery(indent, utils, url_prefix, pipeline_name,program_type,program_id, stats, attempt):
    log.info("____Function: " + str(inspect.stack()[0][3]) + " - Start")
    http_recovery_time  = 60
    max_tries           = 5
    check_status        = "UNKNOWN"
    url                 = "{0}apps/{1}/{2}s/{3}/runs/".format(url_prefix, pipeline_name,program_type,program_id)
    params              = {}
    
    response  = http_call(indent, url, params, "", "get", 0)
    if response.status_code != 200:
        log.info("____Function: " + str(inspect.stack()[0][3]) + " - End")
        return response
    
    runs                = response.json()
    
    log.info(indent + "This trg_control: " + str(stats.trg_ctrl))
    for run in runs:
        runtargs = eval(run["properties"]["runtimeArgs"])
        if runtargs.get("trigger_ctrl") == None: continue
        try:
            tr_ctrl = eval(runtargs["trigger_ctrl"])
        except Exception as e:
            log.error(str(e))
            tr_ctrl = runtargs["trigger_ctrl"]
        
        log.info(indent + "     trg_control: " + str(tr_ctrl))
        if utils.order_dict(tr_ctrl) == utils.order_dict(stats.trg_ctrl):
            log.info(indent + "Comparison: True")
            stats.run_id = run["runid"]
            check_status = stats.status = run["status"].upper()
            break
    
    if check_status == "UNKNOWN" and attempt < max_tries:
        log.info("Attempt # " + str(int(attempt) + 1) + " / " + str(max_tries))
        log.info("Instance status Unknown... waiting " + str(http_recovery_time) + " secs...")
        time.sleep(http_recovery_time)
        response    = df_status_recovery(indent, utils, url_prefix, pipeline_name,program_type,program_id, stats, attempt + 1)
    
    elif check_status == "UNKNOWN" and attempt == max_tries:
        err_msg="Retries exceeded. Instance Not started"
        log.warning(err_msg)
        response.status_code    = 504
        response._content       = err_msg.encode()
    
    else:
        err_msg="Pipeline Instance started"
        log.info(err_msg)
        log.info("Run ID    : " + str(stats.run_id))
        log.info("Status    : " + str(stats.status))
        response._content       = err_msg.encode()
    
    log.info("____Function: " + str(inspect.stack()[0][3]) + " - End")
    return response


#function to check if an instance of the same pipeline is running
#---------------------------------------------------------------------------------------
def df_check_runs(indent, url_prefix,pipeline_name,program_type,program_id):
    log.info("____Function: " + str(inspect.stack()[0][3]) + " - Start")
    check_status        = "UNKNOWN"
    #url                 = "{0}apps/{1}/{2}s/{3}/runs?status=running".format(url_prefix,pipeline_name,program_type,program_id)
    url                 = "{0}apps/{1}/{2}s/{3}/runs/".format(url_prefix,pipeline_name,program_type,program_id)
    params              = {}
    #stats               = {}
   
    response    = http_call(indent, url, params,"","get", 0)
   
    if response.status_code != 200:
        log.info("____Function: " + str(inspect.stack()[0][3]) + " - End")
        return response
   
    runs                = response.json()
   
    running = []
    for run in runs:
        if run["status"] != "COMPLETED" and run["status"] != "FAILED" and run["status"] != "KILLED" and run["status"] != "STOPPED":
            running.append(run)

    if len(running) !=0:
        #log.info("$$Len : " + str(len(running)) + "- Status: " + str(running))
        log.info(indent + "Previous Instance running... waiting " + str(http_poll_wait_time) + " secs...")
        time.sleep(http_poll_wait_time)
        response    = df_check_runs(indent, url_prefix,pipeline_name,program_type,program_id)
    else:
        log.info(indent + "No Previous Instance running.")
   
    log.info("____Function: " + str(inspect.stack()[0][3]) + " - End")
    return response


#function to retrieve data fusion pipeline instance logs
#---------------------------------------------------------------------------------------
def df_retrieve_logs(indent, url_prefix, pipeline_name, program_type, program_id, status, df_run_id, df_start_ts, df_end_ts):
    log.info("____Function: " + str(inspect.stack()[0][3]) + " - Start")
    url_logs        = "{0}apps/{1}/{2}s/{3}/runs/{4}/logs".format(url_prefix,pipeline_name,program_type,program_id,df_run_id)
    get_params      = {"start": df_start_ts, "stop": df_end_ts}
    log.info("url_logs : " + str(url_logs))
    response        = http_call(indent, url_logs, get_params,"","get_log", 0)
    
    if response.status_code != 200:
        log.error("Log Retrieving Fails")
        #response._content   = "No Log Retrieved".encode()

    log.info("____Function: " + str(inspect.stack()[0][3]) + " - End")
    return response


#function to retrieve data fusion pipeline instance stats and information (history)
#---------------------------------------------------------------------------------------
def df_retrieve_stats(indent, url_prefix, pipeline_name, program_type,program_id, stats):
    log.info("____Function: " + str(inspect.stack()[0][3]) + " - Start")
    log.info("Waiting " + str(http_log_wait_time) + " secs for logs aggregation...")
    time.sleep(http_log_wait_time)
    
    check_status        = "UNKNOWN"
    url                 = "{0}apps/{1}/{2}s/{3}/runs/".format(url_prefix,pipeline_name,program_type,program_id)
    params              = {}
    
    response_stats    = http_call(indent, url, params, "", "get", 0)
    if response_stats.status_code != 200:
        log.info("____Function: " + str(inspect.stack()[0][3]) + " - End")
        return stats, response_stats
    
    runs    = response_stats.json()
    flag = 0
    for run in runs:
        #log.info("Run   : " + str(run))
        if run["runid"] == stats.run_id :
            res_json = run
            stats.status              = res_json["status"]
            stats.start_ts            = res_json["starting"]
            stats.end_ts              = res_json["end"]
            stats.run_id              = res_json["runid"]
            stats.response            = "n/a"
            break
    
    if stats.status == "COMPLETED" :
        response_stats._content   = "Pipeline execution completed!".encode()
        #stats.response = "Pipeline execution completed!"
    
    else:
        response_stats.status_code = 1000
        response_log            = df_retrieve_logs(indent, url_prefix, pipeline_name,program_type, program_id, stats.status, stats.run_id, stats.start_ts, stats.end_ts)
        log.info("print 2")
        stats.response          = response_log.text
        response_stats._content = response_log.text.encode()
        log.info("print 3")
   
    log.info("____Function: " + str(inspect.stack()[0][3]) + " - End")
    return stats, response_stats





# function that launch df pipeline
#---------------------------------------------------------------------------------------
def launch_df_pipeline(indent, url_prefix, pipeline_name, program_type, program_id, run_dict, stats):
    log.info("____Function: " + str(inspect.stack()[0][3]) + " - Start")
    url_start           = url_prefix + "start"
    d = [{
        "appId": pipeline_name,
        "programType": program_type,
        "programId": program_id,
        "runtimeargs": run_dict
    }]
    data                = json.dumps(d)
    
    response = http_call(indent, url_start, data, "", "post", 0)
    if response.status_code != 200: 
        log.info("____Function: " + str(inspect.stack()[0][3]) + " - End")
        return response
    
    exec_run_id     = response.json()[-1]["runId"]
    
    stats.run_id = exec_run_id
    log.info(indent + "Execution Run ID      : %s", str(exec_run_id))
    log.info(indent + "Waiting " + str(http_poll_wait_time) + " secs before check job acknowledge")
    #Check job ack
    response    = df_check_status(indent, url_prefix, pipeline_name, program_type, program_id, stats, "ack")
    if response.status_code != 200:
        log.error(indent + "HTTP ack check fails!")
        log.info("____Function: " + str(inspect.stack()[0][3]) + " - End")
        return response
    
    log.info(indent + "Instance of Data Fusion pipeline >" + pipeline_name + "< started!")
    log.info(indent + "Instance start-time: " + datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    
    log.info("____Function: " + str(inspect.stack()[0][3]) + " - End")
    return response
