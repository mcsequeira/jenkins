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
import xml.etree.ElementTree as ET
import datetime
from pathlib import Path
from os import path, remove
import inspect
from retrying import retry

#--------------------------
global success_file_path
global src_ack_file_suffix

global metadata_suffix

global error_folder_label
#global processed_path_suffix
global data_file_prefix
global ack_file_prefix

src_ack_file_suffix         = ".ok"
metadata_suffix             = ".metadata"
data_file_prefix            = "cypher_"
ack_file_prefix             = "PT_RAW_"


#---------------------------------------------------------------------------------------
# Parsing Paths Strings
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

# function that parse gcs extraction partition path as long (year=YYYY/month=MM/day=DD/ as YYYYMMDD)
#---------------------------------------------------------------------------------------
# function that parse gcs extraction partition path as long (year=YYYY/month=MM/day=DD/ as YYYYMMDD)
#---------------------------------------------------------------------------------------
# function that parse gcs file extraction dt as int (YYYYMMDDHHmmss)
#---------------------------------------------------------------------------------------
def parse_file_feed(file):
    log.info("____Function: " + str(inspect.stack()[0][3]) + " - Start")
    try:
        feedname = file.split("/")[-1].split("-")[2]
        log.info("____Function: " + str(inspect.stack()[0][3]) + " - End")
        return feedname
    except:
        log.error("File : " + str(file))
        log.info("\t|_ File not in Scope")
        log.info("____Function: " + str(inspect.stack()[0][3]) + " - End")
        return ""


# function that parse gcs file extraction dt as int (YYYYMMDDHHmmss)
#---------------------------------------------------------------------------------------
def parse_file_succ_raw2raw(file):
    log.info("____Function: " + str(inspect.stack()[0][3]) + " - Start")
    format = "%Y%m%d%H%M%S" #"YYYYMMDDHHmmss"
    file_ts_str         = file.split("-")[-2]
    try:
        datetime.datetime.strptime(file_ts_str, format)
        log.info("____Function: " + str(inspect.stack()[0][3]) + " - End")
        return int(file_ts_str), True
    except:
        log.info("____Function: " + str(inspect.stack()[0][3]) + " - End")
        return 0, False


# function that parse gcs file extraction dt to destination partition path (YYYYMMDD_HHmmss)
#---------------------------------------------------------------------------------------
def parse_dest_extr_path_raw2raw(exec_extr_dt_str):
    log.info("____Function: " + str(inspect.stack()[0][3]) + " - Start")
    format = "%Y%m%d%H%M%S" #"YYYYMMDDHHmmss"
    try:
        datetime.datetime.strptime(exec_extr_dt_str, format)
        dest_path = "year="+exec_extr_dt_str[:4]+"/month="+exec_extr_dt_str[4:6]+"/day="+exec_extr_dt_str[6:8]+"/"
        log.info("____Function: " + str(inspect.stack()[0][3]) + " - End")
        return dest_path        
    except:
        log.error("\t|_Date time not valid for processing")
        log.info("____Function: " + str(inspect.stack()[0][3]) + " - End")
        return ""



# function that parse gcs file extraction dt to destination proc path (YYYYMMDD_HHmmss)
#---------------------------------------------------------------------------------------
def parse_dest_proc_path_raw2raw(exec_extr_dt_str):
    log.info("____Function: " + str(inspect.stack()[0][3]) + " - Start")
    format = "%Y%m%d%H%M%S" #"YYYYMMDDHHmmss"
    try:
        datetime.datetime.strptime(exec_extr_dt_str, format)
        dest_path = str(exec_extr_dt_str[:8]+"_"+exec_extr_dt_str[8:])
        log.info("____Function: " + str(inspect.stack()[0][3]) + " - End")
        return dest_path
    except:
        log.error("\t|_Date time not valid for processing")
        log.info("____Function: " + str(inspect.stack()[0][3]) + " - End")
        return ""


# function that parse gcs xml file (YYYYMMDD_HHmmss)
#---------------------------------------------------------------------------------------
def parse_xml_file_raw2raw(file):
    log.info("____Function: " + str(inspect.stack()[0][3]) + " - Start")
    format = "%Y%m%d%H%M%S" #"YYYYMMDDHHmmss"
    try:
        dt = file.split(".")[-2].split("-")[-2]
        datetime.datetime.strptime(str(dt), format)
        res = int(dt)
        log.info("____Function: " + str(inspect.stack()[0][3]) + " - End")
        return res
    except Exception as e:
        log.warning("\t|_XML file taxonomy not valid!")
        #log.error(str(e))
        log.info("____Function: " + str(inspect.stack()[0][3]) + " - End")
        return 0


# function to retrieve the last xml file from source metadata path
#---------------------------------------------------------------------------------------
def retrieve_last_xml_path(xml_bkt, xml_src_path):
    log.info("____Function: " + str(inspect.stack()[0][3]) + " - Start")
    xml_files   = list(xml_bkt.list_blobs(prefix=xml_src_path, delimiter="/"))
    xml_dt = 0
    res = "/No Metadata File Retrieved"
    for xml in xml_files:
        log.info("\tXML : " + str(xml.name))
        xml_dt_tmp  = parse_xml_file_raw2raw(xml.name)
        if xml_dt_tmp > xml_dt: res = xml.name ; xml_dt = xml_dt_tmp
    res = res.split("/")[-1]
    log.info("\tMetadata XML File : " + str(res))
    
    log.info("____Function: " + str(inspect.stack()[0][3]) + " - End")
    return res

# function to retrieve the xml_schema file from source metadata path
#---------------------------------------------------------------------------------------
@retry(stop_max_attempt_number=5, wait_exponential_multiplier=1000, wait_exponential_max=32000)
def get_xml_schema(bkt, xml_path):    
    log.info("____Function: " + str(inspect.stack()[0][3]) + " - Start")
    blob            = bkt.get_blob(xml_path)
    xml = xml_path.split("/")[-1]
    f = open(xml, 'w+')
    f.write(blob.download_as_string().decode("utf-8"))
    f.close()
    tree = ET.parse(xml)
    root = tree.getroot()
    xml_schema_tmp=[]
    for field in root.iter('field'):
        xml_schema_tmp.append('{"name":"'+field.attrib['name']+'","type":["string","null"]},')
    ## join all array entries into a string
    xml_schema_tmp = ' '.join(map(str, xml_schema_tmp)) 
    ## remove the last ','
    xml_schema_tmp = xml_schema_tmp[:-1]
    ## add the rest of the schema
    xml_schema = '{"type":"record","name":"etlSchemaBody","fields":[' + xml_schema_tmp + ']}'
    if path.exists(xml): remove(xml)
    #log.info("xml_schema: " + print(xml_schema))
    log.info("____Function: " + str(inspect.stack()[0][3]) + " - End")
    return xml_schema

# function to retrieve the last xml file from source metadata path
#---------------------------------------------------------------------------------------
@retry(stop_max_attempt_number=5, wait_exponential_multiplier=1000, wait_exponential_max=32000)
def get_schema(bkt, xml_path):    
    log.info("____Function: " + str(inspect.stack()[0][3]) + " - Start")
    blob            = bkt.get_blob(xml_path)
    xml = xml_path.split("/")[-1]
    f = open(xml, 'w+')
    f.write(blob.download_as_string().decode("utf-8"))
    f.close()
    #blob.download_to_filename(xml)
    #xml = blob.download_as_string()
    tree = ET.parse(xml)
    root = tree.getroot()
    input_schema_tmp=[]
    for neighbor in root.iter('field'):
        field=neighbor.attrib.get('name')
        input_schema_tmp.append(field)
    input_schema = ',:'.join(input_schema_tmp)
    wrangler_command= 'set-headers :'
    input_schema=wrangler_command+input_schema
    if path.exists(xml): remove(xml)
    log.info("____Function: " + str(inspect.stack()[0][3]) + " - End")
    return input_schema
    
xml_path = "ptdmaap/ngbi_client_hierarchy/eis/metadata/PT_RAW-DWH-ngbi_extr_cdc_d_client-20200211000003-metadata.xml"

# function to retrieve the last xml file from source metadata path
#---------------------------------------------------------------------------------------
@retry(stop_max_attempt_number=5, wait_exponential_multiplier=1000, wait_exponential_max=32000)
def get_input_path (bkt, xml_path):
    log.info("____Function: " + str(inspect.stack()[0][3]) + " - Start")
    blob            = bkt.get_blob(xml_path)
    xml = xml_path.split("/")[-1]
    f = open(xml, 'w+')
    f.write(blob.download_as_string().decode("utf-8"))
    f.close()
    #blob.download_to_filename(xml)
    #xml = blob.download_as_string()
    tree = ET.parse(xml)
    root = tree.getroot()
    for neighbor in root.findall("./landing/properties/"):
        if neighbor.attrib.get('name')=='folder':
            if neighbor.attrib.get('value') != None :
                #print(neighbor.attrib.get('value'))
                input_path=neighbor.attrib.get('value')
    if path.exists(xml): remove(xml)
    log.info("____Function: " + str(inspect.stack()[0][3]) + " - End")
    return input_path



#=======================================================================================
# Scope Definition function - RETRIEVE FILE IN SCOPE
#=======================================================================================
#function to retrieve file in scoper stored on gcs
#---------------------------------------------------------------------------------------
def get_files_in_scope_raw2raw(indent, src_bkt, xml_bkt, flow_id, cfg_s):
    log.info("____Function: " + str(inspect.stack()[0][3]) + " - Start")
    src_file_list = []
    src_file_dict = {}
    
    #Retrieving Up to data XML File
    #xml_file  = retrieve_last_xml_path(xml_bkt, cfg_s.metadata)
    
    #update paths variables
    #landing = get_input_path(xml_bkt, cfg_s.metadata + xml_file)
    landing = get_input_path(xml_bkt, cfg_s.metadata)

    cfg_s.get_paths(landing)
    #cfg_s.input_schema = get_schema(xml_bkt, cfg_s.metadata + xml_file)
    #cfg_s.xml_schema = get_xml_schema(xml_bkt, cfg_s.metadata + xml_file)

    cfg_s.input_schema = get_schema(xml_bkt, cfg_s.metadata)
    cfg_s.xml_schema = get_xml_schema(xml_bkt, cfg_s.metadata)
    
    log.info(indent + "Source Folder        : %s", cfg_s.landing)
    log.info(indent + "Processing Folder    : %s", cfg_s.processing)
    log.info(indent + "Archive Folder       : %s", cfg_s.archive)
    log.info(indent + "Error Folder         : %s", cfg_s.errors)
    
    src_blob_list     = list(src_bkt.list_blobs(prefix=cfg_s.landing, delimiter="/"))
    
    # Retrieve File blobs
    #---------------------------------------#
    for file in src_blob_list:
        tag = str(file.name)
        log.info("File: " + tag)
        feedname = parse_file_feed(tag)
        log.info(indent + "|_ feedname :" + feedname)
        if(feedname.upper() == flow_id.upper()):
            log.info(indent + "|_ File in Scope")
            src_file_list.append(tag)
        else: continue

    # Retrieve Data with Success File present
    #---------------------------------------#    
    src_file_list.sort()
    for file in src_file_list:
        if file.endswith(src_ack_file_suffix):
            log.info("")
            log.info("Ack File                      : " + str(file))
            exec_extr_dt, flag = parse_file_succ_raw2raw(file)
            log.info("Date time retrieved           : " + str(exec_extr_dt))
            if not flag:
                log.warning(indent + "|_File not valid")
                continue
            else:
                #log.info("Date time retrieved           : " + str(exec_extr_dt))
                tag                     = str(file.split("/")[-1])
                current_scope_prefix    = str("-".join(tag.split("-")[0:4])) + "-"
                log.info("File in scope prefix (ack)    : " + current_scope_prefix)
                log.info("File in scope prefix (data)   : " + data_file_prefix + current_scope_prefix)
                if src_file_dict.get(exec_extr_dt) == None : src_file_dict[exec_extr_dt] = {}
                src_file_dict[exec_extr_dt]["prefix"] = current_scope_prefix
                
    for exec_extr_dt in src_file_dict.keys():
        log.info("Extr timestamp        : %s", str(exec_extr_dt))
        src_file_prefix = src_file_dict[exec_extr_dt]["prefix"]
        # Retrieve Data with Success File present
        #---------------------------------------#    
        #src_file_dict[exec_extr_dt]["xml"]  = cfg_s.metadata + xml_file
        src_file_dict[exec_extr_dt]["xml"]  = cfg_s.metadata
        src_file_dict[exec_extr_dt]["data"] = []
        
        tgt_path_partition    = parse_dest_extr_path_raw2raw(str(exec_extr_dt))
        #if tgt_path_partition == "": continue
        log.info(indent + "|_Output directory (partition)  : " + tgt_path_partition)
        exec_proc_dt        = parse_dest_proc_path_raw2raw(str(exec_extr_dt))
        #if exec_proc_dt == "": continue
        log.info("Proc timestamp        : %s", str(exec_proc_dt))
        log.info(indent + "|_Output directory (processing) : " + str(exec_proc_dt))
        
        src_file_dict[exec_extr_dt]["tgt"] = [tgt_path_partition, exec_proc_dt]
        
        get_scope_ack_file = ""
        for file in src_file_list:
            tag = file.split("/")[-1]
            
            if tag.startswith(src_file_prefix) and tag.endswith(src_ack_file_suffix):
                log.info("File in scope (ack)    : " + tag)
                src_file_dict[exec_extr_dt]["ack"] = cfg_s.landing + tag
            elif (tag.startswith(data_file_prefix + src_file_prefix) and 
                  (tag.endswith(cfg_s.format) or tag.endswith(cfg_s.format + metadata_suffix))
                  ):
                log.info("File in scope (data)   : " + tag)
                src_file_dict[exec_extr_dt]["data"].append(file)
            else:
                #log.info("File not in scope   : " + str(tag))
                None
    
    log.info("____Function: " + str(inspect.stack()[0][3]) + " - End")
    return src_file_dict

#=======================================================================================
# READ THE OK FILE CONTENTS FUNCTIONS
#=======================================================================================
# function to read the .ok file and get the parameter value
def get_parameter_value(gbl, ok_path):
    log.info("____Function: " + str(inspect.stack()[0][3]) + " - Start")
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(gbl.cfg_g.src_bkt_id)

    #ok_file_path = gbl.cfg_g.src_bkt_id + ok_path 
    blob = bucket.get_blob(ok_path)
    content = blob.download_as_string()
    content = content.decode('utf-8')

    log.info(" ok file contents " + content)
    if content == "":
        log.info(" ok file content is null")
        return ""
    else:
        lines = content.split("\n")

        if "PARAMETER_VALUE" in lines[0]:
            header=lines[0].split(';')
            position = header.index("PARAMETER_VALUE")
            
            values = lines[1].split(';')
            parameter_value = values[position]
            
            log.info("parameter_value:  " + parameter_value)
            return parameter_value
            
        else:
            return ""

    log.info("____Function: " + str(inspect.stack()[0][3]) + " - End")


# function to read the .ok file and get the split_size
def get_split_size(gbl, ok_path):
    log.info("____Function: " + str(inspect.stack()[0][3]) + " - Start")
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(gbl.cfg_g.src_bkt_id)

    blob = bucket.get_blob(ok_path)
    content = blob.download_as_string()
    content = content.decode('utf-8')

    log.info(" ok file contents " + content)
    if content == "":
        log.info(" ok file content is null")
        return ""
    else:
        lines = content.split("\n")
        values=lines[1].split(";")
        if all(x in lines[0] for x in ['ORIGINAL_FILE_SIZE', 'FILE_SIZE_BEFORE_SPLIT']):
            header=lines[0].split(';')
            pos_before_compression=header.index("FILE_SIZE_BEFORE_SPLIT")
            pos_after_compression=header.index("ORIGINAL_FILE_SIZE")
            
            values=lines[1].split(";")
            file_size_before_compression=values[pos_before_compression]
            file_size_after_compression=values[pos_after_compression]

            #determine the # of files delivered by dmaap
            ok_file_name=ok_path.split("/")[-1]
            nr_files=ok_file_name.split(".")[0].split("-")[4]
            log.info("nr_files" + nr_files)

            #compute the split size
            comp_split_size = str(round(((134217728*int(nr_files)*int(file_size_after_compression))/int(file_size_before_compression))))
            log.info("computed split_size:  " + comp_split_size)
            return comp_split_size

        else:
            return ""

    log.info("____Function: " + str(inspect.stack()[0][3]) + " - End")


# function to read the .ok file and get the split_size
def get_min_cutoff_date(gbl, ok_path):
    log.info("____Function: " + str(inspect.stack()[0][3]) + " - Start")
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(gbl.cfg_g.src_bkt_id)

    blob = bucket.get_blob(ok_path)
    content = blob.download_as_string()
    content = content.decode('utf-8')

    log.info(" ok file contents " + content)
    if content == "":
        log.info(" ok file content is null")
        return ""
    else:
        lines = content.split("\n")
        values=lines[1].split(";")
        if all(x in lines[0] for x in ['DT_MIN_CUTOFF']):
            header=lines[0].split(';')
            pos_date_min_cutoff=header.index("DT_MIN_CUTOFF")
            
            values=lines[1].split(";")
            date_min_cutoff=values[pos_date_min_cutoff]

            log.info("OK File min cut-off date:  " + date_min_cutoff)
            return date_min_cutoff

        else:
            return ""

    log.info("____Function: " + str(inspect.stack()[0][3]) + " - End")


def get_max_cutoff_date(gbl, ok_path):
    log.info("____Function: " + str(inspect.stack()[0][3]) + " - Start")
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(gbl.cfg_g.src_bkt_id)

    blob = bucket.get_blob(ok_path)
    content = blob.download_as_string()
    content = content.decode('utf-8')

    log.info(" ok file contents " + content)
    if content == "":
        log.info(" ok file content is null")
        return ""
    else:
        lines = content.split("\n")
        values=lines[1].split(";")
        if all(x in lines[0] for x in ['DT_MAX_CUTOFF']):
            header=lines[0].split(';')
            pos_date_max_cutoff=header.index("DT_MAX_CUTOFF")
            
            values=lines[1].split(";")
            date_max_cutoff=values[pos_date_max_cutoff]

            log.info("OK File max cut-off date:  " + date_max_cutoff)
            return date_max_cutoff

        else:
            return ""

    log.info("____Function: " + str(inspect.stack()[0][3]) + " - End")