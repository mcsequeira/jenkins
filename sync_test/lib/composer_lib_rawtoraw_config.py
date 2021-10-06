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
import json
import inspect

# function
#---------------------------------------------------------------------------------------
def check_not_null(dict_var,key):
    log.info("____Function: " + str(inspect.stack()[0][3]) + " - Start")
    if (dict_var.get(key) != None and dict_var.get(key) != 'None' and dict_var.get(key) != ""): 
        log.info("____Function: " + str(inspect.stack()[0][3]) + " - End")
        return dict_var[key]
    else:
        log.info("____Function: " + str(inspect.stack()[0][3]) + " - End")
        return ""



def get_bkt(bkt_id):
    log.info("____Function: " + str(inspect.stack()[0][3]) + " - Start")
    storage_client  = storage.Client()
    bkt             = storage_client.get_bucket(bkt_id)
    log.info("____Function: " + str(inspect.stack()[0][3]) + " - End")
    return bkt


class Statistics():
    #__slots__ = ["status", "start_ts", "end_ts", "run_id", "run_type", "input_files", "response"]
    status           = "n/a"
    start_ts         = "n/a"
    end_ts           = "n/a"
    run_id           = "n/a"
    run_type         = "n/a"
    input_files      = {}
    response         = "n/a"
    #metrics          = {}
    
    def __init__(self, feeds):
        log.info("____Function: " + str(inspect.stack()[0][3]) + " - Start")
        # Instance Variable  
        self.input_files = {}
        i = 0 
        for file in feeds:
            i = i+1
            self.input_files[i] = file


class Config_general():
    src_prj_id      = "n/a"
    src_bkt_id      = "n/a"
    tgt_prj_id      = "n/a"
    tgt_bkt_id      = "n/a"
    tgt_temp_bucket = "n/a"
    xml_prj_id      = "n/a"
    xml_bkt_id      = "n/a"
    
    df_url          = "n/a"
    project         = "n/a"
    vaultCredentialPath    = "n/a"
    vaultNamespace    = "n/a"
    vaultRole  = "n/a"
    vaultUrl  = "n/a"
    attrib_4  = "n/a"
    
    # The init method or constructor  
    def __init__(self, dict):
        log.info("____Function: " + str(inspect.stack()[0][3]) + " - Start")
        if dict.get("buckets") == None:
            None
        else:
            # Instance Variable  
            self.src_prj_id      = dict["buckets"]["dmaap_prj_id"].rstrip("\r")
            self.src_bkt_id      = dict["buckets"]["dmaap_bkt_id"].rstrip("\r")
            #self.src_bkt         = get_bkt(self.src_bkt_id)

            self.tgt_prj_id      = dict["buckets"]["raw_prj_id"].rstrip("\r")
            self.tgt_bkt_id      = dict["buckets"]["raw_bkt_id"].rstrip("\r")
            #self.tgt_bkt         = get_bkt(self.tgt_bkt_id)
            
            self.xml_prj_id      = dict["buckets"]["meta_xml_prj"].rstrip("\r")
            self.xml_bkt_id      = dict["buckets"]["meta_xml_bkt_id"].rstrip("\r")            
            self.tgt_temp_bucket      = dict["buckets"]["raw_temp_bucket"].rstrip("\r")
            #self.xml_bkt         = get_bkt(self.xml_bkt_id)
            
            self.df_url          = dict["buckets"]["df_url"].rstrip("\r")
            self.project          = dict["buckets"]["project"].rstrip("\r")
            self.vaultCredentialPath    = dict["buckets"]["vaultCredentialPath"].rstrip("\r")
            self.vaultNamespace    = dict["buckets"]["vaultNamespace"].rstrip("\r")
            self.vaultRole  = dict["buckets"]["vaultRole"].rstrip("\r")
            self.vaultUrl  = dict["buckets"]["vaultUrl"].rstrip("\r")

            self.attrib_4  = dict["buckets"]["attrib_4"].rstrip("\r")

class Config_specific():
    metadata               = "n/a"
    target                 = "/tmp"
    tgt_success            = "/tmp/success"
    format                 = "n/a"
    spec_split_size        = "n/a"
    
    output_schema          = "n/a"
    bq_schema              = "n/a"
    tgt_dataset            = "n/a"
    table_name             = "n/a"
    partition_field        = "n/a"
    part_transform         = "n/a"
    trunc_table            = "n/a"
    create_part_table      = "n/a"
    require_part_filter    = "n/a"
    input_transform        = "n/a"
    rawprepared_transform  = "n/a"
    bq_transform           = "n/a"
    
    attrib_2               = "n/a"
    attrib_5               = "n/a"
    attrib_6               = "n/a"
    attrib_7               = "n/a"

    pipeline_name          = "n/a"
    program_type           = "n/a"
    program_id             = "n/a"
    load_directives        = "n/a"
    
    def get_paths(self, path):
        log.info("____Function: " + str(inspect.stack()[0][3]) + " - Start")
        self.landing            = "/".join(path.split("/")[:-2]) + "/landing/"
        self.processing         = "/".join(path.split("/")[:-2]) + "/processing/"
        self.archive            = "/".join(path.split("/")[:-2]) + "/archived/"
        self.errors             = "/".join(path.split("/")[:-2]) + "/errors/" ## error path 

    # The init method or constructor  
    def __init__(self, dict, flow):
        log.info("____Function: " + str(inspect.stack()[0][3]) + " - Start")
        if dict.get("buckets") == None:
            None
        else:
            self.metadata               = dict["buckets"]["mapping_parameter"][flow]["meta_xml_src_path"]
            self.target                 = dict["buckets"]["mapping_parameter"][flow]["output_root_path"]
            self.tgt_success            = "/".join(self.target.split("/")[:-4]) + "/" + "success/"
            self.format                 = dict["buckets"]["mapping_parameter"][flow]["file_format"]            
            self.spec_split_size        = dict["buckets"]["mapping_parameter"][flow]["split_size"]

            self.output_schema          = dict["buckets"]["mapping_parameter"][flow]["output_schema"]
            self.bq_schema              = dict["buckets"]["mapping_parameter"][flow]["bq_schema"]
            self.tgt_dataset            = dict["buckets"]["mapping_parameter"][flow]["dataset"]
            self.table_name             = dict["buckets"]["mapping_parameter"][flow]["table_name"]
            self.partition_field        = dict["buckets"]["mapping_parameter"][flow]["partition_field"]
            self.part_transform         = dict["buckets"]["mapping_parameter"][flow]["part_transform"]
            self.trunc_table            = dict["buckets"]["mapping_parameter"][flow]["trunc_table"]
            self.create_part_table      = dict["buckets"]["mapping_parameter"][flow]["create_part_table"]
            self.require_part_filter    = dict["buckets"]["mapping_parameter"][flow]["require_part_filter"]
            self.input_transform        = dict["buckets"]["mapping_parameter"][flow]["input_transform"]
            self.rawprepared_transform  = dict["buckets"]["mapping_parameter"][flow]["rawprepared_transform"]
            self.bq_transform           = dict["buckets"]["mapping_parameter"][flow]["bq_transform"]

            self.attrib_2               = dict["buckets"]["mapping_parameter"][flow]["attrib_2"]
            self.attrib_5               = dict["buckets"]["mapping_parameter"][flow]["attrib_5"]
            self.attrib_6               = dict["buckets"]["mapping_parameter"][flow]["attrib_6"]
            self.attrib_7               = dict["buckets"]["mapping_parameter"][flow]["attrib_7"]
            
            self.pipeline_name          = dict["buckets"]["df"]["appId"]
            self.program_type           = dict["buckets"]["df"]["programType"]
            self.program_id             = dict["buckets"]["df"]["programId"]
            self.load_directives        = dict["buckets"]["df"]["load_directives"]
            


class Globals_variables():
    composer_layer          = "n/a"
    proc_ts                 = "n/a"
    dag_id                  = "n/a"
    dag_run_id              = "n/a"
    dag_exec_date           = "n/a"
    dag_task                = None
    dag_task_instance       = None
    dag_task_id             = "n/a"
    tgt_interface           = "n/a"
    
    # The init method or constructor  
    def __init__(self,layer,cfg_path,label):
        log.info("____Function: " + str(inspect.stack()[0][3]) + " - Start")
        self.composer_layer     = layer
        self.exec_proc_dt       = "00000000000000"
        self.cfg_bkt            = get_bkt(cfg_path.split("/")[2])
        self.stats              = Statistics(label) #init_stats(label)
