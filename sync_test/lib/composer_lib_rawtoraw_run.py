import logging
import importlib
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
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.exceptions import AirflowSkipException

import google.auth
import google.auth.transport.requests
from google.cloud import storage

from datetime import datetime, timedelta
from pathlib import Path
import traceback
import time
import os
import yaml
import inspect
from retrying import retry

#=======================================================================================
# MAIN FUNCTION
#=======================================================================================
#function to setup the context and trigger the framework
#---------------------------------------------------------------------------------------
def rawtoraw_run(**kwargs):
    log.info("____Function: " + str(inspect.stack()[0][3]) + " - Start")
    #log.info(str(kwargs))
    global gbl
    composer_layer          = "RAWtoRAW"
    
    #for a in os.environ:
    #    print('Var: ', a, 'Value: ', os.getenv(a))
    os.environ["CELERY_LOG_LEVEL"] = "DEBUG"
    #for a in os.environ:
    #    print('Var: ', a, 'Value: ', os.getenv(a))
    
    
    #--------------------------------
    #SETTING CONTEXT VARIABLES
    #--------------------------------
    frmwrk                  = str(kwargs["frmwrk"])
    task_delay              = kwargs["task_delay"]
    trigger_rule            = kwargs["trigger_rule"]
    cfg_gen_path            = kwargs["cfg_gen_path_in"]
    cfg_spec_path           = kwargs["cfg_spec_path_in"]
    flow_id                 = kwargs["source_file"]
    
    log.info("#========================================================================")
    log.info("# Flow ID    : " + flow_id)
    log.info("#========================================================================")
    target_interface        = flow_id
    
    #Inducing Delay
    #--------------------------
    log.info("=================================================")
    if task_delay == 0 or task_delay == "0" or task_delay == "":
        log.info("Staring task with no delay...")
    else:
        log.info("Defering start-time by    : %s.sec", task_delay)
        time.sleep(int(task_delay))    
    log.info("==================================================")
    
    
    # Import Modules
    #--------------------------
    global cfg, errh, poll, http, xcom, utils
    
    try:
        importlib.invalidate_caches()
        http    = importlib.import_module("ngbi." + frmwrk + ".lib.composer_lib_http_requests")
        cfg     = importlib.import_module("ngbi." + frmwrk + ".lib.composer_lib_" + frmwrk + "_config")
        errh    = importlib.import_module("ngbi." + frmwrk + ".lib.composer_lib_" + frmwrk + "_logging")
        poll    = importlib.import_module("ngbi." + frmwrk + ".lib.composer_lib_" + frmwrk + "_polling")
        utils   = importlib.import_module("ngbi." + frmwrk + ".lib.composer_lib_" + frmwrk + "_utils")
    except Exception as e:
        log.error(str(e))
        raise AirflowException("Modules Import Fails")
    
    gbl                     = cfg.Globals_variables(composer_layer,cfg_gen_path,{"feed":"n/a"})
    gbl.trigger_rule        = trigger_rule
    gbl.flow_id             = flow_id
    gbl.tgt_interface       = flow_id
    
    #---------------------------------------------------------------------------------
    #INITIALIZING VARIABLES AND COUNTERS
    #---------------------------------------------------------------------------------
    gbl.proc_ts     = datetime.now(tz=None)
    
    # Airflow DAG information
    #--------------------------
    #gbl.dag_id           = "dag_id"
    #gbl.dag_run_id       = "df_run_id"
    #gbl.dag_exec_date    = "execution_date"
    
    gbl.dag_run             = kwargs["dag_run"]
    gbl.dag_id              = str(kwargs["dag_run"].dag_id)
    gbl.dag_run_id          = str(kwargs["dag_run"].run_id)
    gbl.dag_exec_date       = str(kwargs["dag_run"].execution_date)
    gbl.dag_task            = kwargs["task"]
    gbl.dag_task_instance   = kwargs["ti"]
    gbl.dag_task_id         = gbl.dag_task_instance.task_id
    
    #----------------------------------
    #SOURCE FROM GENERAL CONFIG FILE
    #----------------------------------    
    gbl.cfg_g = cfg.Config_general({})
    gbl.cfg_s = cfg.Config_specific({}, flow_id)
    
    try:
        cfg_gen_dict           = utils.read_yaml_file(cfg_gen_path)
    except Exception as e:
        err_msg = "Config file general not found   :" + cfg_gen_path
        log.error(err_msg)
        log.error(str(e))
        log.error(str(traceback.format_exc()))
        errh.logging_manager(gbl, "n/a", "n/a", "n/a", 100, err_msg)
        raise AirflowException("Execution Error")
    
    utils.log_dict("General Control Sheet",cfg_gen_dict,2,"",1,True)
    cfg_g = gbl.cfg_g = cfg.Config_general(cfg_gen_dict)
    
    # GCS
    #--------------------------
    log.info("--------------------------------------------------------------------------")
    log.info("Source Bucket Name   : %s", cfg_g.src_bkt_id)
    log.info("Target Bucket Name   : %s", cfg_g.tgt_bkt_id)
    log.info("--------------------------------------------------------------------------")
    log.info("")
    try:                             
        gbl.src_bkt         = utils.get_bkt(cfg_g.src_bkt_id)
        gbl.tgt_bkt         = utils.get_bkt(cfg_g.tgt_bkt_id)
        gbl.xml_bkt         = utils.get_bkt(cfg_g.xml_bkt_id)
    except Exception as e:
        log.error(str(e))
        log.error(str(traceback.format_exc()))
        errh.logging_manager(gbl,"n/a","n/a","n/a",100,str(e))
        raise AirflowException("Execution Error")
    
    
    #----------------------------------
    #SOURCE FROM SPECIFIC CONFIG FILE
    #----------------------------------    
    try:
        cfg_spec_dict    = utils.read_yaml_file(cfg_spec_path)
    except Exception as e:
        err_msg = "Config file specific not found   :" + cfg_spec_path
        log.error(err_msg)
        log.error(str(e))
        log.error(str(traceback.format_exc()))
        errh.logging_manager(gbl, "n/a", "n/a", "n/a", 100, err_msg)
        raise AirflowException("Execution Error")
    
    utils.log_dict("Specific Control Sheet",cfg_spec_dict,2,"",1,True)
    gbl.cfg_s = cfg.Config_specific(cfg_spec_dict, flow_id)
    #gbl.tgt_interface = flow_id
    
    log.info("--------------------------------------------------------------------------")
    log.info("Pipeline Name        : %s", gbl.cfg_s.pipeline_name)
    log.info("Program Type         : %s", gbl.cfg_s.program_type)
    log.info("Program Id           : %s", gbl.cfg_s.program_id)
    log.info("")
    #log.info("Source Folder        : %s", gbl.cfg_s.landing)
    #log.info("Processing Folder    : %s", gbl.cfg_s.processing)
    #log.info("Archive Folder       : %s", gbl.cfg_s.archive)
    #log.info("Error Folder         : %s", gbl.cfg_s.errors)
    log.info("Xml Metadata Folder  : %s", gbl.cfg_s.metadata)
    log.info("Target Root Path     : %s", gbl.cfg_s.target)
    log.info("File Format          : %s", gbl.cfg_s.format)
    
    log.info("--------------------------------------------------------------------------")
    log.info("")
    
    
    #-----------------------------------
    # RAW to RAW specififc
    #-----------------------------------
    
    df_pipeline_exec_framework_raw2raw(gbl)



#=======================================================================================
# CORE SOFTWARE PACKAGES
#=======================================================================================
# MANAGE ORCHESTRATION
#---------------------------------------------------------------------------------------
# function that manage orchestration of DataFusion piepelines
#---------------------------------------------------------------------------------------
def df_pipeline_exec_framework_raw2raw(gbl):
    log.info("____Function: " + str(inspect.stack()[0][3]) + " - End")
    indent = "\t"
    
    gbl.src_file_dict = poll.get_files_in_scope_raw2raw(indent, gbl.src_bkt, gbl.xml_bkt, gbl.flow_id, gbl.cfg_s)
    
    utils.log_dict("Scope",gbl.src_file_dict,2,"",1,True)
    
    #if len(gbl.src_file_list) == 0:
    if len(gbl.src_file_dict.keys()) == 0:
        err_msg = "No file to process"
        log.info(err_msg)
        errh.logging_manager(gbl,"n/a","n/a","NONE",201,err_msg)
        log.info("____Function: " + str(inspect.stack()[0][3]) + " - End")
        return    
    
    else:
        for exec_extr_dt in gbl.src_file_dict.keys():    ## get the csv file for every success file and move to processing.
            df_pipeline_exec_manager_raw2raw(indent, gbl, exec_extr_dt)


#function to manage pipeline triggering execution
#---------------------------------------------------------------------------------------
def df_pipeline_exec_manager_raw2raw(indent, gbl, exec_extr_dt):
    log.info("____Function: " + str(inspect.stack()[0][3]) + " - Start")
    log.info("<> Processing date: " + str(exec_extr_dt))
    src_dict = gbl.src_file_dict[exec_extr_dt]
    # get the .ok file contente here
    log.info("Ok file to get the PARAMETER_VALUE from: " + src_dict["ack"])
    parameter_value = poll.get_parameter_value(gbl, src_dict["ack"])
    gbl.cfg_s.param_value = parameter_value

    #determine the split size (from the ok file or specific file)
    ok_split_size = poll.get_split_size(gbl, src_dict["ack"])
    if ok_split_size == "":
        final_split_size = gbl.cfg_s.spec_split_size 
    else:
        final_split_size = ok_split_size

    gbl.cfg_s.split_size = final_split_size

    log.info("Final split_size: " + gbl.cfg_s.split_size)

    # get min cutoff date
    ok_min_cutoff_date = poll.get_min_cutoff_date(gbl, src_dict["ack"])
    gbl.cfg_s.min_cutoff_date = ok_min_cutoff_date

    log.info("Min cutoff date: " + gbl.cfg_s.min_cutoff_date)

    # get max cutoff date
    ok_max_cutoff_date = poll.get_max_cutoff_date(gbl, src_dict["ack"])
    gbl.cfg_s.max_cutoff_date = ok_max_cutoff_date

    log.info("Max cutoff date: " + gbl.cfg_s.max_cutoff_date)




    processing_flag     = 0
    if len(src_dict["data"]) == 0:
        err_msg = "No data file to process"
        log.warning(err_msg)
        errh.logging_manager(gbl,str(exec_extr_dt),"n/a","NONE",202,err_msg)
        #Only one ack file allowed for each extraction_dt
        arch_file_list, err_msg, mv_code    = utils.moving_files(gbl.cfg_g.src_bkt_id, [src_dict["ack"]], gbl.cfg_g.src_bkt_id, gbl.cfg_s.archive, "archive")
        log.warning("Processing of date time " + str(exec_extr_dt) + " ended! No data file retrieved.")
        log.error("Proceeding with the next date time")
        log.info("#========================================================================")
        log.info("____Function: " + str(inspect.stack()[0][3]) + " - End")
        return
            
    ## move files to processing folder. 
    ## before that, assess whether there are files already in processing. If so, throw an error
    proc_blob_list     = list(gbl.src_bkt.list_blobs(prefix=gbl.cfg_s.processing, delimiter="/"))
    log.info("proc_blob_list length:" + str(len(proc_blob_list)))
    if len(proc_blob_list) > 0:
        prc_file_to_move = []
        for file in proc_blob_list:
            prc_file_to_move.append(str(file.name))
            log.error("File in processing to move to errors: " + str(file.name))

        # move files in processing to errors
        prc_file_list, err_msg_prc, mv_code_prc    = utils.moving_files(gbl.cfg_g.src_bkt_id, prc_file_to_move, gbl.cfg_g.src_bkt_id, gbl.cfg_s.errors, "errors")

        # abort the run
        proc_err_msg = "Files in processing folder! They were moved to the errors folder and the job aborted!"
        log.error(proc_err_msg)
        raise AirflowException(proc_err_msg) 

    ## no files in processing. Proceed.
    src_dict["data"], err_msg, mv_code    = utils.moving_files(gbl.cfg_g.src_bkt_id, src_dict["data"], gbl.cfg_g.src_bkt_id, gbl.cfg_s.processing, "processing")
    log.info("proc_file_list : " + str(src_dict["data"]))
    if mv_code != 0 :
        #log.error(err_msg)
        errh.logging_manager(gbl,str(exec_extr_dt),str(src_dict["tgt"][1]).replace("_",""),"ERROR",mv_code,err_msg)
        log.error("Processing of date time " + str(exec_extr_dt) + " failed!")
        log.error("Proceeding with the next date time")
        log.info("#========================================================================")
        log.info("____Function: " + str(inspect.stack()[0][3]) + " - End")
        return 
    
    
    gbl.stats, response     = launch_df_pipeline_raw2raw(indent, gbl, exec_extr_dt, src_dict)
    log.info("Status of the pipeline " + gbl.cfg_s.pipeline_name + "." + gbl.flow_id + " for "+ str(exec_extr_dt) + "  is " + gbl.stats.status)
    log.info("#------------------------------------------------------------------------")
    
    if gbl.stats.status == "COMPLETED":
        #Creating success file
        #succ_code, succ_msg      = utils.create_success_file_raw2raw(gbl, src_dict["tgt"], str(exec_extr_dt)[0:8]) ## create success file
        succ_code, succ_msg      = utils.create_success_file_raw2raw(gbl, src_dict["tgt"], exec_extr_dt) ## create success file
        if succ_code != 0:
            errh.logging_manager(gbl,str(exec_extr_dt),str(src_dict["tgt"][1]).replace("_",""),"ERROR",succ_code,succ_msg)
            err_msg = "Success Folder Creation Failed"
            log.error("Processing of date time " + str(exec_extr_dt) + " failed! - " + err_msg)
            log.error("Move Processing files to archive folder before next run!")
            log.info("#========================================================================")
            raise AirflowException(err_msg) 
            #continue
        
        ##Copying XML Metadata File from Source Bucket to Target Bucket
        #copy_flag = utils.copy_blob(gbl.cfg_g.xml_bkt, gbl.cfg_s.metadata + src_dict["xml"], gbl.tgt_bkt, gbl.cfg_s.xml_tgt_path + src_dict["xml"])
        #if copy_flag:
        #    log.info("XML Metadata File Copying: Success!")
        #else:
        #    err_msg = "XML Metadata File Copying: Failed!"
        #    errh.logging_manager(gbl, str(exec_extr_dt),str(src_dict["tgt"][1]).replace("_",""), "WARNING", copy_flag, err_msg)
        #    log.error("Processing of date time " + str(exec_extr_dt) + " failed! - " + err_msg)
            
    
    if gbl.stats.status == "COMPLETED":
        mv_folder   = gbl.cfg_s.archive #+ src_dict["tgt"][1] + "/"
        label       = "archive"
    else: 
        mv_folder   = gbl.cfg_s.errors #+ src_dict["tgt"][1] + "/"
        label       = "errors"
    src_dict["data"].append(src_dict["ack"])    
    
    ## archive/reject the processing files for respective extract dates
    src_dict["data"], err_msg, mv_code    = utils.moving_files(gbl.cfg_g.src_bkt_id, src_dict["data"], gbl.cfg_g.src_bkt_id, mv_folder, label)
    if mv_code != 0:
        errh.logging_manager(gbl, str(exec_extr_dt), str(src_dict["tgt"][1]).replace("_",""), "ERROR", mv_code, err_msg)
        err_msg = "File Moving Failed"
        log.error("Processing of date time " + str(exec_extr_dt) + " failed! - " + err_msg)
        log.error("Move Processing files to archive folder before next run!")
        raise AirflowException(err_msg) 
            
    errh.logging_manager(gbl, str(exec_extr_dt), str(src_dict["tgt"][1]).replace("_",""), gbl.stats.status, response.status_code, response.text)
    log.error("Processing of date time " + str(exec_extr_dt) + " : " + str(gbl.stats.status))
    if gbl.stats.status != "COMPLETED": raise AirflowException("Execution Error")
    log.error("Proceeding with the next date time")
    log.info("#========================================================================")
            
    log.info("____Function: " + str(inspect.stack()[0][3]) + " - End")
    return 




#=======================================================================================
# DataFusion Pipeline Triggering
#=======================================================================================
#function that manage datafusion pipeline execution
#---------------------------------------------------------------------------------------
def launch_df_pipeline_raw2raw(indent, gbl, exec_extr_dt, src_dict):
    log.info("____Function: " + str(inspect.stack()[0][3]) + " - Start")
    stats =  gbl.stats = cfg.Statistics(src_dict["data"])    
    
    log.info(indent + "Proceeding with execution...")
    run_dict, stats.trg_ctrl    = define_runargs_raw2raw(gbl, src_dict["tgt"], src_dict["xml"])
    utils.log_dict("Runtime args", run_dict,2, indent, 1,True)
    
    log.info(indent + "+--------------------------------------------------------------------------")
    log.info(indent + "| Executing pipeline " + gbl.cfg_s.pipeline_name + "." + gbl.flow_id)
    log.info(indent + "+--------------------------------------------------------------------------")

    attempt         = 1
    #trg_code        = 200
    trigger_retries = 3
    
    stats, trg_response = trigger_pipe(indent, stats, run_dict)
    while attempt <= trigger_retries and trg_response.status_code != 200:
        log.info("Trigger attempt: " + str(attempt + 1) + " / " + str(trigger_retries + 1))
        stats, trg_response = trigger_pipe(indent, stats, run_dict)
        #trg_code = trg_response.status_code
        attempt += 1
    
    if trg_response.status_code != 200: return stats, trg_response
    
    errh.logging_manager(gbl, str(exec_extr_dt), str(src_dict["tgt"][1]).replace("_",""), "STARTING", 202, "Starting Application")
    
    #Check job status
    response    = http.df_check_status(indent, gbl.cfg_g.df_url, gbl.cfg_s.pipeline_name, gbl.cfg_s.program_type, gbl.cfg_s.program_id, stats, "status")
    if response.status_code != 200:
        log.error(indent + "HTTP call fails!")
        stats.response   = response.text
        log.info("____Function: " + str(inspect.stack()[0][3]) + " - End")
        return stats, response
    
    stats, response = http.df_retrieve_stats(indent, gbl.cfg_g.df_url, gbl.cfg_s.pipeline_name, gbl.cfg_s.program_type, gbl.cfg_s.program_id, stats)
    
    log.info("____Function: " + str(inspect.stack()[0][3]) + " - End")
    return stats, response


def trigger_pipe(indent, stats, run_dict):
    log.info("____Function: " + str(inspect.stack()[0][3]) + " - End")
    #Launching Application
    trg_response    = http.launch_df_pipeline(indent, gbl.cfg_g.df_url, gbl.cfg_s.pipeline_name, gbl.cfg_s.program_type, gbl.cfg_s.program_id, run_dict, stats)
    if trg_response.status_code != 200:
        log.warning("HTTP call status >>UNKNOWN<<. Check if the Pipeline was triggered")
        response = http.df_status_recovery(indent, utils, gbl.cfg_g.df_url, gbl.cfg_s.pipeline_name, gbl.cfg_s.program_type, gbl.cfg_s.program_id, stats, 0)
        stats.response  = trg_response.text
    
    log.info("____Function: " + str(inspect.stack()[0][3]) + " - End")
    return stats, trg_response


# function that manage datafusion pipeline execution
#---------------------------------------------------------------------------------------
def define_runargs_raw2raw(gbl, tgt_paths, xml_path):
    log.info("____Function: " + str(inspect.stack()[0][3]) + " - Start")
    run_dict={}
    run_dict["input.Project"]        = gbl.cfg_g.src_prj_id
    run_dict["input.Directory"]      = "gs://" +  gbl.cfg_g.src_bkt_id + "/" + gbl.cfg_s.processing # + tgt_paths[1] + "/"
    run_dict["extract_dt"]           = tgt_paths[1]
                                     
    run_dict["meta_xml_prj"]         = gbl.cfg_g.xml_prj_id
    run_dict["meta_xml_bkt_id"]      = gbl.cfg_g.xml_bkt_id
    run_dict["meta_xml_src_path"]    = xml_path    
                                     
    run_dict["split_size"]           = gbl.cfg_s.split_size
    run_dict["input_schema"]         = gbl.cfg_s.input_schema
    run_dict["xml_schema"]           = gbl.cfg_s.xml_schema
    run_dict["param_value"]          = gbl.cfg_s.param_value

    run_dict["output_schema"]        = gbl.cfg_s.output_schema
    run_dict["bq_schema"]            = gbl.cfg_s.bq_schema
    run_dict["output.Dataset"]       = gbl.cfg_s.tgt_dataset
    run_dict["table_name"]           = gbl.cfg_s.table_name
    run_dict["partition_field"]      = gbl.cfg_s.partition_field
    run_dict["part_transform"]       = gbl.cfg_s.part_transform
    run_dict["trunc_table"]          = gbl.cfg_s.trunc_table
    run_dict["create_part_table"]    = gbl.cfg_s.create_part_table
    run_dict["require_part_filter"]  = gbl.cfg_s.require_part_filter
    
    run_dict["input_transform"]      = gbl.cfg_s.input_transform
    run_dict["rawprepared_transform"] = gbl.cfg_s.rawprepared_transform
    run_dict["bq_transform"]         = gbl.cfg_s.bq_transform
                                     
    run_dict["output.Project"]       = gbl.cfg_g.tgt_prj_id
    run_dict["temp.Bucket"]          = gbl.cfg_g.tgt_temp_bucket
    run_dict["output.directory"]     = "gs://" + gbl.cfg_g.tgt_bkt_id  + "/" + gbl.cfg_s.target + tgt_paths[0]

    run_dict["project"]              = gbl.cfg_g.project
    run_dict["vaultCredentialPath"]  = gbl.cfg_g.vaultCredentialPath
    run_dict["vaultNamespace"]       = gbl.cfg_g.vaultNamespace
    run_dict["vaultRole"]            = gbl.cfg_g.vaultRole
    run_dict["vaultUrl"]             = gbl.cfg_g.vaultUrl												 
    run_dict["get_data"]             = gbl.cfg_s.load_directives

    run_dict["attrib_4"]             = gbl.cfg_g.attrib_4

    run_dict["attrib_2"]             = gbl.cfg_s.attrib_2
    run_dict["attrib_5"]             = gbl.cfg_s.attrib_5
    run_dict["attrib_6"]             = gbl.cfg_s.attrib_6
    run_dict["attrib_7"]             = gbl.cfg_s.attrib_7

    run_dict["start_cutoff"]         = gbl.cfg_s.min_cutoff_date
    run_dict["end_cutoff"]           = gbl.cfg_s.max_cutoff_date



    #trigger control
    trigger_ctrl = {
                        "env":{
                            "project_id"        : os.environ["GCP_PROJECT"],
                            "location"          : os.environ["COMPOSER_LOCATION"],
                            "environment_name"  : os.environ["COMPOSER_ENVIRONMENT"]
                            },
                        "workflow"          : gbl.dag_run.dag_id,
                        "execution-date"    : gbl.dag_run.execution_date.strftime("%Y-%m-%dT%H:%M:%S.%f%z"),
                        "dag_run_id"        : gbl.dag_run.run_id,
                        "task-id"           : gbl.dag_task_instance.task_id,
                        "ts"                : datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f%z")
                    }
    
    run_dict["trigger_ctrl"]    = str(trigger_ctrl)
    
    #run_dict["trigger_ctrl"]    = "composer : " + str(os.environ["COMPOSER_ENVIRONMENT"]) + " - task: " + gbl.dag_task_id + " - instance: " + gbl.dag_exec_date
    
    log.info("____Function: " + str(inspect.stack()[0][3]) + " - End")
    return run_dict, trigger_ctrl
