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
import google.auth
import google.auth.transport.requests
from google.cloud import storage

from datetime import datetime, timedelta
from pathlib import Path
import time
import yaml
import os
import inspect
from retrying import retry


global log_max_text_leng
log_max_text_leng           = 300



#=======================================================================================
# Utility functions
#=======================================================================================
#---------------------------------------------------------------------------------------
# GCS Utilities
#---------------------------------------------------------------------------------------
# function that provide the authentication on gcs
#---------------------------------------------------------------------------------------
#@retry(stop_max_attempt_number=5, wait_exponential_multiplier=1000, wait_exponential_max=32000)
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


# function
#---------------------------------------------------------------------------------------
#@retry(stop_max_attempt_number=5, wait_exponential_multiplier=1000, wait_exponential_max=32000)
def get_bkt(bkt_id):
    log.info("____Function: " + str(inspect.stack()[0][3]) + " - Start")
    storage_client  = storage.Client()
    bkt             = storage_client.get_bucket(bkt_id)
    log.info("____Function: " + str(inspect.stack()[0][3]) + " - End")
    return bkt



#---------------------------------------------------------------------------------------
# Parsing Paths Strings
#---------------------------------------------------------------------------------------
# function that parse path string to retrieve base name of level "lev"
#---------------------------------------------------------------------------------------
def parse_path_basename(path,lev):
    log.info("____Function: " + str(inspect.stack()[0][3]) + " - Start")
    if path.split("/")[-1] == "":
        tag = path.split("/")[-(lev+1)]
    else:
        tag = path.split("/")[-lev]
    log.info("____Function: " + str(inspect.stack()[0][3]) + " - End")
    return tag


# function that parse path string to retrieve path of level "lev"
#---------------------------------------------------------------------------------------
def parse_path(path,lev):
    log.info("____Function: " + str(inspect.stack()[0][3]) + " - Start")
    leng = len(path.split("/"))
    tag = "/".join(path.split("/")[0:(leng-lev)])
    log.info("____Function: " + str(inspect.stack()[0][3]) + " - End")
    return tag


# function that parse path string to retrieve path of level "lev"
#---------------------------------------------------------------------------------------
def parse_string_len(string,leng):
    log.info("____Function: " + str(inspect.stack()[0][3]) + " - Start")
    value = str(string)
    if len(value) > leng:
        tag = value[0:(leng-3)] + "..."
    else:
        tag = value
    log.info("____Function: " + str(inspect.stack()[0][3]) + " - End")
    return tag


# function that print dict information
#---------------------------------------------------------------------------------------
def log_dict(label,dictionary,layer,indent,k_len,header):
    log.info("____Function: " + str(inspect.stack()[0][3]) + " - Start")
    leng = k_len
    for key in dictionary.keys(): 
        leng = max(int(leng),len(str(key)))
    leng = str(leng)
    if header:
        log.info(indent + "+-----------------------------------------------------------------")
        log.info(indent + "| " + label)
        log.info(indent + "+-----------------------------------------------------------------")
    
    for key in dictionary: 
        if layer == 1:
            value = parse_string_len(str(dictionary[key]),log_max_text_leng)
            log.info(indent + "| " + (("{:" + leng + "}").format(str(key))) + " : [" + str(value) + "]")
        elif layer == 2:
            if isinstance(dictionary[key],dict):
                log.info(indent + "|_" + ("{:" + leng + "}").format(str(key)) + " : ")
                log_dict(key,dictionary[key],layer,indent + (" " * int(leng)),k_len,False)
            else:
                try: 
                    val = str(("{:" + leng + "}").format(int(dictionary[key])))
                except:
                    val = str(dictionary[key])
                
                value = parse_string_len(str(val),log_max_text_leng)
                log.info(indent + "|_ " + (("{:" + leng + "}").format(str(key))) + " : [" + str(value) + "]")
    if header:
        log.info(indent + "+-----------------------------------------------------------------")



#function that check if threshold is exceeded
#---------------------------------------------------------------------------------------
def check_threshold(indent, compared_ts,curr_ts,hour_threshold):
    log.info("____Function: " + str(inspect.stack()[0][3]) + " - Start")
    #extr_dt_src_format      = "%Y%m%d"
    threshold_ts_src_format     = "%H:%M:%S"
    threshold_ts_dst_format     = "%H%M%S"
    compared_dt_dst_format      = "%Y%m%d"
    
    curr_ts_string          = curr_ts.strftime(compared_dt_dst_format + threshold_ts_dst_format)
    curr_ts_int             = int(curr_ts_string)
    log.info(indent + "Execution date        : %d", curr_ts_int)
    
    threshold_string        = datetime.strptime(hour_threshold, threshold_ts_src_format).strftime(threshold_ts_dst_format)
    threshold_ts_string     = compared_ts + threshold_string
    threshold_ts_int        = int(threshold_ts_string)
    log.info(indent + "Threshold Time        : %d", threshold_ts_int)
    
    if curr_ts_int >= threshold_ts_int:
        log.info(indent + "Threshold Check       : Success!")
        log.info("____Function: " + str(inspect.stack()[0][3]) + " - End")
        return True
    else:
        log.error(indent + "Threshold Check  : Failed!")
        log.info("____Function: " + str(inspect.stack()[0][3]) + " - End")
        return False



# function that reads YAML file from GCS
#---------------------------------------------------------------------------------------
#@retry(stop_max_attempt_number=5, wait_exponential_multiplier=1000, wait_exponential_max=32000)
def read_yaml_file(yaml_file):
    log.info("____Function: " + str(inspect.stack()[0][3]) + " - Start")
    bucket_name     = yaml_file.split("/")[2]
    filepath        = "/".join(yaml_file.split("/")[3:])
    
    #get_auth_token()
    storage_client  = storage.Client()
    bucket          = storage_client.get_bucket(bucket_name)
    blob            = bucket.get_blob(filepath)
    config_data     = yaml.safe_load(blob.download_as_string())
    log.info("____Function: " + str(inspect.stack()[0][3]) + " - End")
    return config_data


#function
#---------------------------------------------------------------------------------------
def order_dict(obj):
    log.info("____Function: " + str(inspect.stack()[0][3]) + " - Start")
    if isinstance(obj, dict):
        log.info("____Function: " + str(inspect.stack()[0][3]) + " - End")
        return sorted((k, order_dict(v)) for k, v in obj.items())
    if isinstance(obj, list):
        return sorted(order_dict(x) for x in obj)
    else:
        log.info("____Function: " + str(inspect.stack()[0][3]) + " - End")
        return obj


## function that copies a blob from one bucket to another with a new name.
##---------------------------------------------------------------------------------------
#@retry(stop_max_attempt_number=5, wait_exponential_multiplier=1000, wait_exponential_max=32000)
#def copy_blob(src_bkt,src_blob_name,tgt_bkt,tgt_blob_name):
#    log.info("____Function: " + str(inspect.stack()[0][3]) + " - Start")
#    src_blob = src_bkt.blob(src_blob_name)
#    try:
#        log.info("Copying Blob {} in bucket {} to blob {} in bucket {}.".format(
#            src_blob.name,src_bkt.name,
#            tgt_blob_name,tgt_bkt.name
#            ))
#        blob_copy = src_bkt.copy_blob(src_blob, tgt_bkt, tgt_blob_name)
#        log.info("____Function: " + str(inspect.stack()[0][3]) + " - End")
#        return True
#    except Exception as e:
#        log.error(str(e))
#        log.info("____Function: " + str(inspect.stack()[0][3]) + " - End")
#        return False


# function to create ack file on dest path
#---------------------------------------------------------------------------------------
#@retry(stop_max_attempt_number=5, wait_exponential_multiplier=1000, wait_exponential_max=32000)
def create_success_file_raw2raw(gbl, data_path, exec_extr_dt):
    log.info("____Function: " + str(inspect.stack()[0][3]) + " - Start")
    target_ack_file_suffix      = ".Success"
    dataproc_ack_file_suffix    = "_SUCCESS"
    
    log.info("Creating destination success file")
    path_prefix         = "gs://" + gbl.cfg_g.tgt_bkt_id + "/" 
    success_file_row    = path_prefix + gbl.cfg_s.target + data_path[0] + data_path[1] + "/"
    success_file_path   = gbl.cfg_s.tgt_success + gbl.flow_id + "_" + str(exec_extr_dt) + target_ack_file_suffix
    success_file_tag    = success_file_path.split("/")[-1]
    print_success       = []
    log.info("Updating Success File : " + success_file_path)
    try:
        gbl.tgt_bkt.get_blob(success_file_path).download_to_filename(success_file_tag)
        log.info("Success file just present. Appending new processing path.")
    except :
        log.info("Success file not present. New file will be created for current date.")
    
    with open(success_file_tag, mode="a") as succ_file:
        succ_file.write(success_file_row + "\n")
        
    #    print_success.append(my_new_file.read())
    #succ_data     = ""
    ##succ_path            = dest_path.split("/",3)[1:][-1] + "/"
    #succ_blobs           = list(gbl.tgt_bkt.list_blobs(prefix=gbl.cfg_s.tgt_success, delimiter="/"))
    
    #for file in succ_blobs:
    #    if file.name.endswith(dataproc_ack_file_suffix):
    #        #succ_data    = succ_blobs[2]
    #        log.error("succ_blobs " + str(succ_blobs))
    #        succ_data    = succ_blobs[2]
    #        break            
    
    #log.info("Appending Path    : " + str(succ_data.name))
    #file = "/".join(succ_data.name.split("/")[:-1])
    #path_prefix = "gs://" + gbl.tgt_bkt_name + "/" 
    #print_success.append(path_prefix + file + "/")
    #print_final_data            = "\n".join(print_success)
    try:
        blob_folder = gbl.tgt_bkt.blob(success_file_path)
        #Append to present
        blob_folder.upload_from_filename(success_file_tag)
        log.info("Success file updated successfully at " + success_file_path)
    except Exception as err_msg:
        log.info("Success file not created please check below error")
        log.info(str(err_msg))
        log.info("____Function: " + str(inspect.stack()[0][3]) + " - End")
        return 100, str(err_msg)
    
    try:
        os.remove(success_file_tag)
    except:
        pass
    
    log.info("____Function: " + str(inspect.stack()[0][3]) + " - End")
    return 0, ""


@retry(stop_max_attempt_number=5, wait_exponential_multiplier=1000, wait_exponential_max=32000)
def copy_file_cust(src_bkt, source_blob, dest_bucket, dest_file):
    src_bkt.copy_blob(source_blob, dest_bucket, new_name=dest_file)
    return 0


# function to archive files processed
#---------------------------------------------------------------------------------------
#@retry(stop_max_attempt_number=5, wait_exponential_multiplier=1000, wait_exponential_max=32000)
def moving_files(src_bkt_id, src_file_list, tgt_bkt_id, dest_dir, label):

    log.info("____Function: " + str(inspect.stack()[0][3]) + " - Start")
    dst_file_dict = {}
    log.info("Moving files to " + label + " folder...")

    source_bucket = get_bkt(src_bkt_id)
    dest_bucket = get_bkt(tgt_bkt_id)
    
    for blob_files in src_file_list:
        dst_file_dict[blob_files] = blob_files
        tag = str(blob_files.split("/")[-1])
        log.info("Moving File: " + str(tag) + " ...")
        
        initial_value = 0
        
        source_blob = source_bucket.get_blob(blob_files)
        
        try:
            copy_file_cust(source_bucket, source_blob, dest_bucket, dest_dir + blob_files.split("/")[-1])
            log.info("\t|_" + " - File Copied to Target location")
        except Exception as e:
            log.info("\t|_" + " - Error while copying")
            log.info(e)
            initial_value = 1
        
        try:
            target_blob = dest_bucket.get_blob(
                dest_dir + blob_files.split("/")[-1])
        
        except Exception as e:
            log.info("\t|_" + " - File not present in Target location")
            log.info(e)
            initial_value = 1
        
        if initial_value == 0:
            try:
                source_bucket.delete_blob(source_blob.name)
                log.info("\t|_" + " - File Deleted in Source Location")
            except Exception as e:
                initial_value = 1
                log.info(e)
        
        if initial_value == 0:
            log.info("\t|_" + " - Success")
            dst_file_dict[blob_files] = dest_dir + tag
            log.info("\t|_ - Moving process Completed")
        else:
            log.error("\t|_ - Failed")
            err_msg = "Moving process Fails"
            #if label == "archive":
            #log.warning("\t|_Move the file manually to " + label + " if pipeline has been executed successfully ---------------")
            log.warning("\t|_Move the file manually to " + label)
            log.info("____Function: " + str(inspect.stack()[0][3]) + " - End")
            return list(dst_file_dict.values()), err_msg, 1301
    log.info("____Function: " + str(inspect.stack()[0][3]) + " - End")
    return list(dst_file_dict.values()), "", 0

