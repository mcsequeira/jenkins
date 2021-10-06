import logging
#=======================================================================================
# Create Logger
#=======================================================================================
logger_name = "ngbi_composer_rawtoraw"
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
import google.cloud.logging # Don't conflict with standard logging
from google.cloud.logging import Resource
import time
import os
import json
import re
import inspect
from retrying import retry

global gbl
global res

client = google.cloud.logging.Client()
logger = client.logger(logger_name)
#log.info("os.environ: " +str(os.environ))
res = Resource(
    type="cloud_composer_environment",
    labels={
        "project_id"        : os.environ["GCP_PROJECT"],
        "location"          : os.environ["COMPOSER_LOCATION"],
        "environment_name"  : os.environ["COMPOSER_ENVIRONMENT"]
    }
)



#=======================================================================================
# LOG FILE API
#=======================================================================================
#log file context
#--------------------------
global log_col_dag_id
global log_col_run_id
global log_col_dag_exec_dt
global log_col_dag_task
global log_col_df_target_interface
                       
global log_col_df_pipeline_name
global log_col_dh_extr_dt
global log_col_dh_proc_dt
global log_col_task_proc_ts
global log_col_df_status
global log_col_df_exit_code
global log_col_df_run_id
global log_col_df_start_ts
global log_col_df_end_ts
global log_col_df_run_type
global log_col_df_input_files
global log_col_delimiter

log_col_dag_id              = "dag_id"
log_col_run_id              = "dag_run_id"
log_col_dag_exec_dt         = "dag_exec_date"
log_col_dag_task            = "dag_task_id"
log_col_df_target_interface = "target_interface"
                                     
log_col_df_pipeline_name    = "pipeline_name"
log_col_dh_extr_dt          = "datahub_extr_dt"
log_col_dh_proc_dt          = "datahub_proc_dt"
log_col_task_proc_ts        = "proc_ts"
log_col_df_status           = "status"
log_col_df_exit_code        = "exit_code"
log_col_df_run_id           = "df_run_id" 
log_col_df_start_ts         = "df_start_ts"
log_col_df_end_ts           = "df_end_ts"
log_col_df_run_type         = "df_run_type"
log_col_df_input_files      = "input_files"
log_col_delimiter           = ";"



#=======================================================================================
# ERROR HANDLING
#=======================================================================================
#function to build the managed errors structure
#---------------------------------------------------------------------------------------
def exit_code_mapping():
    log.info("____Function: " + str(inspect.stack()[0][3]) + " - Start")
    #code,          exception,                           description
    exit_code_list = [
        ( 100,     "UnknownError",                        "Error Unknown"                                             ),
        ( 200,     "",                                    "null"                                                      ),
        ( 201,     "NoFileToProcess",                     "No File to process"                                        ),
        ( 205,     "ThresholdExceeded",                   "File Skipped. No Master file Found and Threshold Exceeded" ),
        ( 210,     "RunNone",                             "Run None"                                                  ),
        
        (1000,    "DataFusion Pipeline Error",            "DataFusione Pipeline Error"                                ),
        (1001,    "java.io.IOException",                  "Input directory or file not available"                     ),
        (1002,    "java.lang.Exception",                  "Wrangler transformation failed"                            ),
        (1003,    "InvalidMacroException",                "Run Time Argument not available"                           ),
        (1004,    "DirectiveLoadException",               "Missing user defined directives"                           ),
        (1005,    "UnexpectedFormatException",            "Null Values in not-null columns"                           ),
        (1006,    "NumberFormatException",                "Datatype Mismatch"                                         ),
        (1007,    "ValidationException",                  "Big Query table not present"                               ),
        (1008,    "java.lang.IllegalArgumentException",   "Schema Mismatch"                                           ),
        
        (1100,    "DataProc error",                       "DataProc error"                                            ),
        (1101,    "HTTPRequestFail",                      "HTTP Connection Timeout"                                   ),
        (1102,    "InstanceUnreachable",                  "Data fusion Instance is not available or temporarily down" ),
        (1103,    "PipelineNotFound",                     "Pipeline Not Found"                                        ),
        
        ]
    
    err_dict = {}
    for err in exit_code_list:
        exit_code = err[0]
        err_dict[exit_code] = {
        "type":err[1],
        "desc":err[2]
        }
    
    log.info("____Function: " + str(inspect.stack()[0][3]) + " - End")
    return err_dict


#function to log Audit information on Stack Driver
#---------------------------------------------------------------------------------------
@retry(stop_max_attempt_number=5, wait_exponential_multiplier=1000, wait_exponential_max=32000)
def stack_driver_logging(
    gbl,
    dh_extr_dt,
    dh_proc_dt,
    status,
    exit_code,
    err_ts
    ):
    log.info("____Function: " + str(inspect.stack()[0][3]) + " - Start")
    json_log = {
        gbl.composer_layer + " - Audit_Details":
            { 
                "dag_id"           : gbl.dag_id,
                "dag_run_id"       : gbl.dag_run_id,
                "dag_exec_date"    : gbl.dag_exec_date,
                "dag_task_id"      : gbl.dag_task_id,
                "target_interface" : gbl.tgt_interface,
                
                "pipeline_name"    : gbl.cfg_s.pipeline_name,
                "dh_extr_dt"       : str(dh_extr_dt),
                "dh_proc_dt"       : str(dh_proc_dt),
                "proc_ts"          : str(gbl.proc_ts),
                "status"           : status,
                "df_run_type"      : gbl.stats.run_type,
                "df_start_ts"      : str(gbl.stats.start_ts),
                "df_end_ts"        : str(gbl.stats.end_ts),
                "df_run_id"        : str(gbl.stats.run_id),
                "input_files"      : str(gbl.stats.input_files),
                "exit_code"        : str(exit_code),
                "err_ts"           : str(err_ts),
                "error_type"       : str(exit_code_mapping_dict[exit_code]["type"]),
                "error_desc"       : str(exit_code_mapping_dict[exit_code]["desc"])#,
                #"metrics"          : str(gbl.stats.metrics)
            }
        }
    
    label = {
              "layer"           : gbl.composer_layer,
              "type"            : "Audit_Details",
              "workflow"        : gbl.dag_id,
              "task-id"         : gbl.dag_task_id,
              "execution-date"  : gbl.dag_exec_date
              }
    
    log.info(str(json_log))
    logger.log_struct(json_log, resource = res,labels = label)
    
    log.info("____Function: " + str(inspect.stack()[0][3]) + " - End")
    return


#function to manage logging features
#---------------------------------------------------------------------------------------
def logging_manager(gbl, dh_extr_dt, dh_proc_dt, status, exit_code, response_text):
    log.info("____Function: " + str(inspect.stack()[0][3]) + " - Start")
    
    if exit_code == 200 and status == "FAILED": exit_code = 1000
    
    
    if exit_code != 200 and exit_code != 1103:
        if exit_code_mapping_dict.get(exit_code) == None:
            exit_code_mapping_dict[exit_code] = {}
            exit_code_mapping_dict[exit_code]["type"] = "UnknownError"
        
        for code in exit_code_mapping_dict.keys():
            if code == 200: continue
            
            err_type = exit_code_mapping_dict[code]["type"]
            #match = re.search(r"((.*){1})" + re.escape(err_type) + "((.*:){1})((.*\n){1})", response_text.rstrip("\r"), re.IGNORECASE)
            match = re.search(r"(((.*) - ERROR (.*)){1})" + re.escape(err_type) + "((:){1})((.*\n){1})", response_text.rstrip("\r"), re.IGNORECASE)
            if match:
                exit_code = code
                exit_code_mapping_dict[exit_code]["desc"] = match.group(0)
                break
            else:
                match = re.search(r"(((.*) - ERROR (.*)){1}(\n){1}(.*){1})" + re.escape(err_type) + "((:){1})((.*\n){1})", response_text.rstrip("\r"), re.IGNORECASE)            
                if match:
                    exit_code = code
                    exit_code_mapping_dict[exit_code]["desc"] = match.group(0)
                    break
        
        if not match:
            err_type = "Caused by"
            exit_code_mapping_dict[exit_code]["desc"] = "Unknown" #response_text[:100]
            #reg = re.search(r"((.*){1})" + re.escape(err_type) + "((.*:){1})((.*\n){1})", response_text.rstrip("\r"), re.IGNORECASE)
            reg = re.search(re.escape(err_type) + "((: ){1})((.*){1})((: ){1})((.*\n){1})", response_text.rstrip("\r"), re.IGNORECASE)
            if reg:
                exit_code_mapping_dict[exit_code]["type"] = reg.group(3)
                exit_code_mapping_dict[exit_code]["desc"] = reg.group(0)
                
            else:
                err_type = "Exception:"
                reg = re.search(r"((.*){1})" + re.escape(err_type) + "((.*\n){1})", response_text.rstrip("\r"), re.IGNORECASE)
                if reg:
                    exit_code_mapping_dict[exit_code]["type"] = reg.group(1)
                    exit_code_mapping_dict[exit_code]["desc"] = reg.group(0)
    
    stack_driver_logging(gbl, dh_extr_dt, dh_proc_dt, status, exit_code, str(int(time.time())))
    
    log.info("____Function: " + str(inspect.stack()[0][3]) + " - End")
    return



global exit_code_mapping_dict
global error_type
global error_desc

exit_code_mapping_dict      = exit_code_mapping()
error_type                  = "null"
error_desc                  = "null"
