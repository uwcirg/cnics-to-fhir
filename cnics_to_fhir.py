# -*- coding: utf-8 -*-
"""
Created on Thu Jul 22 13:01:49 2021

@author: James Sibley
"""

import configparser, csv, datetime, logging, mysql.connector, orjson, re, requests, time
from requests.exceptions import ConnectionError, HTTPError, Timeout

def dx_to_coding_code(dx_text):
    if re.search("^[A-Z][0-9]{2}", dx_text) and not re.search("^V", dx_text):
        return dx_text
    elif re.search("^[0-9]{3}", dx_text) or re.search("^V[0-9]{2}", dx_text):
        return dx_text
    elif dx_text in CNICS_STANDARD_DIAGNOSES:
        return dx_text
    else:
        return "404684003"

def dx_to_coding_display(dx_text):
    if re.search("^[A-Z][0-9]{2}", dx_text) and not re.search("^V", dx_text):
        return dx_text
    elif re.search("^[0-9]{3}", dx_text) or re.search("^V[0-9]{2}", dx_text):
        return dx_text
    elif dx_text in CNICS_STANDARD_DIAGNOSES:
        return dx_text
    else:
        return "Clinical finding (finding): " + dx_text

def dx_to_coding_system(dx_text):
    if re.search("^[A-Z][0-9]{2}", dx_text) and not re.search("^V", dx_text):
        return "http://hl7.org/fhir/sid/icd-10-cm"
    elif re.search("^[0-9]{3}", dx_text) or re.search("^V[0-9]{2}", dx_text):
        return "http://hl7.org/fhir/sid/icd-9-cm"
    elif dx_text in CNICS_STANDARD_DIAGNOSES:
        return "https://cnics.cirg.washington.edu/diagnosis-name"
    else:
        return "http://snomed.info/sct"

def med_to_status(start_date, end_date, end_type):
    if start_date is not None:
        if end_date is not None:
            return "stopped"
        return "active"
    return "unknown"

def pro_sql_gen(obj_type, session_id):
    if obj_type == 5:
#        print("select * from ProAltered where PatientId = '" + pat_id + "' order by ProId")
        return """
select distinct p.PatientID, p.MRN
from Patients p
join Sessions s on p.PatientID = s.PatientID
where s.SessionID = '""" + session_id + """'
"""

def setup_logger(name, log_file, level=logging.INFO):
    """To setup as many loggers as you want"""

    handler = logging.FileHandler(log_file)        
    handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)-8s %(message)s'))

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)

    return logger

def sql_connect(cnxn_type = 1, site = '', db_name = ''):
    if cnxn_type == 1:
        # Connection to CNICS database to pull CNICS data resources
        if site == 'uw':
            db_pw = SECRETS['Database']['DataPw_uw'].strip('"')
            db_port = SETTINGS['Database']['DataPort_uw'].strip('"')
        else:
            db_pw = SECRETS['Database']['DataPw_non-uw'].strip('"')
            db_port = SETTINGS['Database']['DataPort_non-uw'].strip('"')

        cnxn = mysql.connector.connect(user = SETTINGS['Database']['DataUser'].strip('"'),
                                       password = db_pw,
                                       host = SETTINGS['Database']['DataHost'].strip('"'),
                                       port = db_port,
                                       database = db_name)
    else:
        # Connection to "Reveal" database to pull MRN identifiers from PRO database
        if site in ['jh', 'fenway']:
            pro_db_user = SETTINGS['Database']['ProUserPrefix_external'].strip('"')
            pro_db_pw = SECRETS['Database']['ProPw_' + site].strip('"')
            pro_db_port = SETTINGS['Database']['ProPort_' + site].strip('"')
            pro_db_name = SETTINGS['Database']['ProDbPrefix_external'].strip('"')
        else:
            pro_db_user = SETTINGS['Database']['ProUserPrefix_local'].strip('"') + site
            pro_db_pw = SECRETS['Database']['ProPw_' + site].strip('"')
            pro_db_port = SETTINGS['Database']['ProPort_local'].strip('"')
            pro_db_name = SETTINGS['Database']['ProDbPrefix_local'].strip('"') + site

        cnxn = mysql.connector.connect(user = pro_db_user,
                                       password = pro_db_pw,
                                       host = SETTINGS['Database']['ProHost'].strip('"'),
                                       port = pro_db_port,
                                       database = pro_db_name)

    return cnxn

def sql_gen(obj_type, pat_id, site, site_pat_id):
    if obj_type == 0:
#        print("select * from Patient where Site = '" + site + "' and SitePatientId = '" + site_pat_id + "'")
        return """
select *
from Patient
where Site = '""" + site + """'
and SitePatientId = '""" + site_pat_id + """'
"""
    elif obj_type == 1:
#        print("select * from DiagnosisAltered where PatientId = '" + pat_id + "' order by DiagnosisId")
        return """
select *
from DiagnosisAltered
where PatientId = '""" + pat_id + """'
and (Historical <> 'Yes' or Historical is NULL)
and length(DiagnosisName) > 0
and """ + conditions_filter
    elif obj_type == 2:
#        print("select * from DemographicAltered where PatientId = '" + pat_id + "' order by DemographicId")
        return """
select *
from DemographicAltered
where PatientId = '""" + pat_id + """'
order by DemographicId
"""
    elif obj_type == 3:
#        print("select * from MedicationAltered where PatientId = '" + pat_id + "' order by MedicationId")
        return """
select *
from MedicationAltered
where PatientId = '""" + pat_id + """'
and (Historical <> 'Yes' or Historical is NULL)
and length(MedicationName) > 0
and """ + medications_filter
    elif obj_type == 4:
#        print("select * from ProAltered where PatientId = '" + pat_id + "' order by ProId")
        return """
select distinct SessionId
from ProAltered
where PatientId = '""" + pat_id + """'
"""
    elif obj_type == 5:
#        print("select * from LabAltered where PatientId = '" + pat_id + "' order by LabId")
        return """
select *
from LabAltered
where PatientId = '""" + pat_id + """'
and (Historical <> 'Yes' or Historical is NULL)
and length(TestName) > 0
and """ + observations_filter

def sql_run(query, cnxn_type, site, db_name):
    retry_flag = True
    retry_count = 0
    cnxn = sql_connect(cnxn_type, site, db_name)
    cursor = cnxn.cursor()
    while retry_flag and retry_count < 5:
        try:
            cursor.execute(query)
            retry_flag = False

        except Exception as e:
            debug_logger.debug(e)
            debug_logger.debug("Retrying in 5 sec...")
            retry_count = retry_count + 1
            cursor.close()
            cnxn.close()
            time.sleep(5)
            cnxn = sql_connect(cnxn_type, site, db_name)
            cursor = cnxn.cursor()

    return cursor.fetchall()

SETTINGS = configparser.ConfigParser()
SETTINGS.read('./settings.ini')
SECRETS = configparser.ConfigParser()
SECRETS.read('./secrets.ini')
JOB_LIST = configparser.ConfigParser()
JOB_LIST.read('./job-config.ini')

info_logger = setup_logger('info_logger', SETTINGS['Logging']['LogPath'].strip('"') + "cnics_to_fhir.log")
debug_logger = setup_logger('debug_logger', SETTINGS['Logging']['LogPath'].strip('"') + "cnics_to_fhir_verbose_debug.log", logging.DEBUG)

with open(SETTINGS['Files']['StandardDiagnoses'].strip('"')) as f:
    CNICS_STANDARD_DIAGNOSES = [i.replace('"', '') for i in f.read().splitlines()]
with open(SETTINGS['Files']['StandardMedications'].strip('"')) as f:
    CNICS_STANDARD_MEDICATIONS = [i.replace('"', '') for i in f.read().splitlines()]

fhir_store = SETTINGS['Options']['FhirStore'].strip('"')
if fhir_store == "hapi":
    fhir_store_path = SETTINGS['Options']['HapiFhirUrl'].strip('"')
    fhir_query_headers = {"Content-Type": "application/fhir+json;charset=utf-8"}
elif fhir_store == "aidbox":
    fhir_store_path = SETTINGS['Options']['AidboxFhirUrl'].strip('"')
    fhir_auth_token = None
    fhir_auth_response = None
    fhir_auth_headers = {'Content-Type': 'application/json'}
    fhir_auth_params = {'grant_type': 'client_credentials', 'client_id': 'client-cnics-crud', 'client_secret': SECRETS['FHIR']['AidboxAuthPw'].strip('"')}
    fhir_auth_response = requests.post(SETTINGS['Options']['AidboxAuthUrl'].strip('"'), headers = fhir_auth_headers, params = fhir_auth_params)
    if fhir_auth_response is not None:
        reply = fhir_auth_response.json()
        debug_logger.debug(reply)

        fhir_query_headers = {'Authorization': 'Bearer ' + reply['access_token'], 'Content-Type': 'application/json'}
    else:
        info_logger.info("Unable to query FHIR server for auth token.")
        quit()

# Set a maximum number of resources to return in FHIR queries
# Note this is a temporary hack, paginating results should be implemented instead
fhir_max_count = "50000"

# Read in filters to limit data pulled from CNICS db
conditions_filter = SETTINGS['Filters']['ConditionsFilter'].strip('"')
medications_filter = SETTINGS['Filters']['MedicationsFilter'].strip('"')
observations_filter = SETTINGS['Filters']['ObservationsFilter'].strip('"')

# Field mapping dicts
dx_to_category = {
                  "Data collected at CNICS site": "encounter-diagnosis",
                  "Patient reported without supporting outside documentation": "health-concern",
                  "Reported in outside documentation": "problem-list-item",
                  "Source unknown": "health-concern",
                  "Verified clinical diagnosis": "problem-list-item"
                 }
category_code_to_display = {
                            "encounter-diagnosis": "Encounter Diagnosis",
                            "health-concern": "Health Concern",
                            "problem-list-item": "Problem List Item",
                            "16100001": "Death Diagnosis"                            
                           }
dx_to_verification_status = {
                             "Data collected at CNICS site": "confirmed",
                             "Patient reported without supporting outside documentation": "unconfirmed",
                             "Reported in outside documentation": "confirmed",
                             "Source unknown": "unconfirmed",
                             "Verified clinical diagnosis": "confirmed"
                            }

# Open a session to the FHIR endpoint instead of making individual calls as this typically speeds things up significantly
session = requests.Session()

# Loop over job list lines
job_cnt = 1
while 'Job_' + str(job_cnt) in JOB_LIST['JobList']:
    job_line = JOB_LIST['JobList']['Job_' + str(job_cnt)].strip('"').split(":")
    site_list = job_line[0].split(",")
    db_name = job_line[1]
    resource_list = job_line[2].split(",")

    for site in site_list:
        pat_id_list = []
        pat_vals = sql_run("""
        select *
        from Patient p
        join DemographicAltered d on p.PatientId = d.PatientId
        where Site = '""" + site + """'
        #order by rand()
        limit """ + SETTINGS['Options']['PatCnt'].strip('"') + """
        """, 1, site, db_name)
        
        pat_id_fn = SETTINGS['Logging']['LogPath'].strip('"') + "cnics_to_fhir_pid_list.txt"
        pat_id_file = open(pat_id_fn, "w", encoding="utf-8")
        for i in range(0, len(pat_vals)):
          pat_id_file.write(str(pat_vals[i][2]) + ':' + pat_vals[i][1].decode("utf-8").replace("'", "''") + '\n')
        pat_id_file.close()
        
        with open(pat_id_fn) as pat_id_file:
            pat_id_lines = [line.rstrip() for line in pat_id_file]
        
        info_logger.info("==================================================================")
        info_logger.info("Run Date/Time: " + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        info_logger.info(fhir_store_path)
        info_logger.info("Resource List: " + ",".join(resource_list))
        info_logger.info("Site: " + site)
        info_logger.info("Database Name: " + db_name)
        info_logger.info("Patient Count: " + str(len(pat_id_lines)))
        
        for i in range(0, len(pat_id_lines)):
            pat_id_list.append(tuple([str(pat_id_lines[i].split(":")[0]), pat_id_lines[i].split(":")[1]]))
        
        # Read in MRNs from an external file to use as additional identifiers for patient resources
        # This is currently only for the "UW" site, will need modifications to accommodate other sites
        site_id_mrns = {}
        if site == 'uw':
            cnt = 0
            mrn_file = SETTINGS['Files']['Mrns' + site.upper()].strip('"')
            with open(mrn_file, newline='') as csvfile:
                spamreader = csv.reader(csvfile, delimiter=',', quotechar='"')
                for row in spamreader:
                    if cnt != 0:
                        if str(row[2]) != 'NULL':
                            if str(row[2]) not in site_id_mrns.keys():
                                site_id_mrns[str(row[2])] = {}
                            site_id_mrns[str(row[2])]['hmrn'] = str(row[0])
                            if str(row[1]) != 'NULL':
                                site_id_mrns[str(row[2])]['umrn'] = str(row[1])
                    cnt += 1
        
        # Query for patient data, xform to FHIR resource, upload to HAPI as insert (if new) or update (if existing)
        bench_start = datetime.datetime.now()
        
        total_pat_del = 0
        total_pat_ins = 0
        total_pat_upd = 0
        total_dx_del = 0
        total_dx_ins = 0
        total_dx_upd = 0
        total_med_del = 0
        total_med_ins = 0
        total_med_upd = 0
        total_lab_del = 0
        total_lab_ins = 0
        total_lab_upd = 0
        
        # Collect current patients in FHIR store for the site to look for any that need to be deleted
        response = session.get(fhir_store_path + "/Patient?identifier=https://cnics.cirg.washington.edu/site-patient-id/" + site + "|&_format=json&_count=" + fhir_max_count, headers = fhir_query_headers)
        response.raise_for_status()
        reply = response.json()
        debug_logger.debug(reply)
        
        if "entry" in reply:
            # Delete any existing patients with no matching current entry
            for l in range(0, len(reply["entry"])):
                pat = reply["entry"][l]
                if pat["resource"]["identifier"][0]["value"] not in [x[1] for x in pat_id_list]:
                    response = session.delete(fhir_store_path + "/Patient/" + pat["resource"]["id"] + "?_cascade=delete", headers = fhir_query_headers)
                    response.raise_for_status()
                    del_reply = response.json()
                    total_pat_del = total_pat_del + 1
                    debug_logger.debug(del_reply)
        
        for i in range(0, len(pat_id_list)):
            pat_vals = sql_run(sql_gen(0, None, pat_id_list[i][0], pat_id_list[i][1]), 1, site, db_name)
            pat_id = str(pat_vals[0][0])
            
            dx_vals = sql_run(sql_gen(1, pat_id, None, None), 1, site, db_name)
            
            demo_vals = sql_run(sql_gen(2, pat_id, None, None), 1, site, db_name)
        
            med_vals = sql_run(sql_gen(3, pat_id, None, None), 1, site, db_name)
        
            sess_vals = sql_run(sql_gen(4, pat_id, None, None), 1, site, db_name)
        
            lab_vals = sql_run(sql_gen(5, pat_id, None, None), 1, site, db_name)
        
            # See if patient resource already exists, get ID if yes
            response = session.get(fhir_store_path + "/Patient?identifier=https://cnics.cirg.washington.edu/site-patient-id/" + pat_id_list[i][0].lower() + "|" + str(pat_vals[0][1].decode("utf-8")) + "&_format=json&_count=" + fhir_max_count, headers = fhir_query_headers)
            response.raise_for_status()
            reply = response.json()
            debug_logger.debug(reply)
        
            if reply["total"] < 2:
                # Insert or update
                if reply["total"] == 1:
                    hapi_pat_id = reply["entry"][0]["resource"]["id"]
                else:
                    hapi_pat_id = None
        
                # Populate the bones of a patient resource
                pat_resource = {
                                     "resourceType": "Patient",
                                     "meta": { "profile": ["http://hl7.org/fhir/us/core/StructureDefinition/us-core-patient"] },
                                     "text": {
                                              "status": "generated",
                                              "div": "<div xmlns=\"http://www.w3.org/1999/xhtml\">Generated by CIRG's CNICS to FHIR. Version identifier: 0.1</div>"
                                             },
                                     "extension": [],
                                     "identifier": []
                               }
                
                if hapi_pat_id is not None:
                    pat_resource["id"] = hapi_pat_id
                    total_pat_upd = total_pat_upd + 1
                else:
                    total_pat_ins = total_pat_ins + 1
                    
                # Fill in identifiers, if any
                if pat_vals[0][1] is not None:
                    pat_resource["identifier"].append({
                                                                   "system": "https://cnics.cirg.washington.edu/site-patient-id/" + pat_id_list[i][0].lower(),
                                                                   "value": str(pat_vals[0][1].decode("utf-8"))
                                                                  })
                # All SessionId values from the ProAltered table to make it easier to match patients with the PRO system
                for j in range(0, len(sess_vals)):
                    pat_resource["identifier"].append({
                                                                   "system": "https://cnics-pro.cirg.washington.edu/session-id/" + pat_id_list[i][0].lower(),
                                                                   "value": sess_vals[j][0]
                                                                  })
                
                # MRNs from the local clinic site, if provided in a separate file
                if str(pat_vals[0][1].decode("utf-8")) in site_id_mrns.keys():
                    if 'hmrn' in site_id_mrns[str(pat_vals[0][1].decode("utf-8"))].keys():
                        pat_resource["identifier"].append({
                                                                       "system": "https://cnics-pro.cirg.washington.edu/institution-mrn/" + pat_id_list[i][0].lower(),
                                                                       "value": site_id_mrns[str(pat_vals[0][1].decode("utf-8"))]['hmrn']
                                                                      })
                    if 'umrn' in site_id_mrns[str(pat_vals[0][1].decode("utf-8"))].keys():
                        pat_resource["identifier"].append({
                                                                       "system": "https://cnics-pro.cirg.washington.edu/institution-mrn/" + pat_id_list[i][0].lower(),
                                                                       "value": site_id_mrns[str(pat_vals[0][1].decode("utf-8"))]['umrn']
                                                                      })
                else: # Get MRN and PRO PatientID values from the PRO system
                    uniq_pro_pat_ids = []
                    uniq_pro_mrns = []
                    for j in range(0, len(sess_vals)):
                        id_vals = sql_run(pro_sql_gen(5, sess_vals[j][0]), 2, site, db_name)
                        for k in range(0, len(id_vals)):
                            if id_vals[k][0] is not None:
                                if id_vals[k][0] not in uniq_pro_pat_ids:
                                    uniq_pro_pat_ids.append(id_vals[k][0])
                            if id_vals[k][1] is not None:
                                if id_vals[k][1] not in uniq_pro_mrns:
                                    uniq_pro_mrns.append(id_vals[k][1])
        
                    for uniq_id in uniq_pro_pat_ids:
                        pat_resource["identifier"].append({
                                                                       "system": "https://cnics-pro.cirg.washington.edu/pro-patient-id/" + pat_id_list[i][0].lower(),
                                                                       "value": str(uniq_id)
                                                                      })
                    for uniq_id in uniq_pro_mrns:
                        pat_resource["identifier"].append({
                                                                       "system": "https://cnics-pro.cirg.washington.edu/institution-mrn/" + pat_id_list[i][0].lower(),
                                                                       "value": uniq_id
                                                                      })
        
                # Look for demographic info for the patient, add to patient resource, if any
                pat_race_code = ""
                pat_race_display = ""
                pat_eth_code = ""
                pat_eth_display = ""
                pat_birth_sex = ""
                pat_birth_date = ""
                for j in range(0, len(demo_vals)):
                    if demo_vals[j][3] != int(pat_id):
                        continue
                    else:
                        if demo_vals[j][7] is not None: # Race
                            if demo_vals[j][7] == "American Indian":
                                pat_race_code = "1002-5"
                                pat_race_display = "American Indian or Alaska Native"
                            elif demo_vals[j][7] == "Asian":
                                pat_race_code = "2028-9"
                                pat_race_display = "Asian"
                            elif demo_vals[j][7] == "Asian/Pacific Islander":
                                pat_race_code = "2076-8"
                                pat_race_display = "Native Hawaiian or Other Pacific Islander"
                            elif demo_vals[j][7] == "Black":
                                pat_race_code = "2054-5"
                                pat_race_display = "Black or African American"
                            elif demo_vals[j][7] == "Pacific Islander":
                                pat_race_code = "2076-8"
                                pat_race_display = "Native Hawaiian or Other Pacific Islander"
                            elif demo_vals[j][7] == "White":
                                pat_race_code = "2106-3"
                                pat_race_display = "White"
                            elif demo_vals[j][7] == "Multiracial":
                                pat_race_code = "2131-1"
                                pat_race_display = "Other Race"
                            elif demo_vals[j][7] == "Other":
                                pat_race_code = "2131-1"
                                pat_race_display = "Other Race"
                            if pat_race_code != "" and pat_race_display != "":
                                pat_race = {
                                            "url": "http://hl7.org/fhir/us/core/StructureDefinition/us-core-race",
                                            "extension": [{
                                                           "url": "ombCategory",
                                                           "valueCoding": {
                                                                           "system": "urn:oid:2.16.840.1.113883.6.238",
                                                                           "code": pat_race_code,
                                                                           "display": pat_race_display
                                                                          }
                                                          },
                                                          {
                                                           "url": "text",
                                                           "valueString": pat_race_display
                                                          }]
                                           }
                                pat_resource["extension"].append(pat_race)
            
                        if demo_vals[j][8] is not None: # Ethnicity
                            if demo_vals[j][8] == "No":
                                pat_eth_code = "2186-5"
                                pat_eth_display = "Non Hispanic or Latino"
                            elif demo_vals[j][8] == "Yes":
                                pat_eth_code = "2135-2"
                                pat_eth_display = "Hispanic or Latino"
                            if pat_eth_code != "" and pat_eth_display != "":
                                pat_eth = {
                                           "url": "http://hl7.org/fhir/us/core/StructureDefinition/us-core-ethnicity",
                                           "extension": [{
                                                          "url": "ombCategory",
                                                          "valueCoding": {
                                                                          "system": "urn:oid:2.16.840.1.113883.6.238",
                                                                          "code": pat_eth_code,
                                                                          "display": pat_eth_display
                                                                         }
                                                         },
                                                         {
                                                          "url": "text",
                                                          "valueString": pat_eth_display
                                                         }]
                                          }
                                pat_resource["extension"].append(pat_eth)
            
                        if demo_vals[j][6] is not None:
                            if demo_vals[j][6] == "Female":
                                pat_birth_sex = "F"
                            elif demo_vals[j][6] == "Male":
                                pat_birth_sex = "M"
                            if pat_birth_sex != "":
                                pat_sex = {
                                           "url": "http://hl7.org/fhir/us/core/StructureDefinition/us-core-birthsex",
                                           "valueCode": pat_birth_sex
                                          }
                                pat_resource["extension"].append(pat_sex)
                                pat_resource["gender"] = demo_vals[j][6].lower()
            
                        break

                debug_logger.debug(orjson.dumps(pat_resource, option = orjson.OPT_NAIVE_UTC | orjson.OPT_INDENT_2).decode("utf-8"))
                        
#                headers = {"Content-Type": "application/fhir+json;charset=utf-8"}
                if hapi_pat_id is not None:
                    response = session.put(fhir_store_path + "/Patient/" + hapi_pat_id, headers = fhir_query_headers, json = pat_resource)
                else:
                    response = session.post(fhir_store_path + "/Patient", headers = fhir_query_headers, json = pat_resource)
                response.raise_for_status()
                resource = response.json()
                debug_logger.debug(resource)
        
                if hapi_pat_id is None:
                    hapi_pat_id = resource['id']
                
                # If selected, collect current condition resources for the patient
                if "conditions" in resource_list:
                    response = session.get(fhir_store_path + "/Condition?subject=" + "Patient/" + hapi_pat_id + "&_format=json&_count=" + fhir_max_count, headers = fhir_query_headers)
                    response.raise_for_status()
                    reply = response.json()
                    debug_logger.debug(reply)
                    
                    if "entry" in reply:
                        cond_entry_actions = [None] * len(reply["entry"])
                        
                        # Delete any existing condition resources with no matching current diagnosis
                        for l in range(0, len(reply["entry"])):
                            cond = reply["entry"][l]
                            for k in range(0, len(dx_vals)):
                                if "identifier" in cond["resource"].keys():
                                    if str(dx_vals[k][4].decode("utf-8")) == cond["resource"]["identifier"][0]["value"]:
                                        cond_entry_actions[l] = "update"
                                        break
                                    else:
                                        cond_entry_actions[l] = "delete"
                                    
                        for ind in range(0, len(cond_entry_actions)):
                            if cond_entry_actions[ind] == "delete":
                                response = session.delete(fhir_store_path + "/Condition/" + reply["entry"][ind]["resource"]["id"], headers = fhir_query_headers)
                                response.raise_for_status()
                                del_reply = response.json()
                                total_dx_del = total_dx_del + 1
                                debug_logger.debug(del_reply)
                    else:
                        cond_entry_actions = []
                    
                    # Insert any new diagnoses without existing condition resource or update, if existing
                    for k in range(0, len(dx_vals)):
                        if dx_vals[k][3] != int(pat_id) and dx_vals[k][7].strip() != '':
                            continue
                        else:
                            api_call = "POST"
                            if "entry" in reply:
                                for cond in reply["entry"]:
                                    if "identifier" in cond["resource"].keys():
                                        if str(dx_vals[k][4].decode("utf-8")) == cond["resource"]["identifier"][0]["value"]:
                                            api_call = "PUT"
                                            break
            
                            # Populate the bones of a condition resource
                            cond_resource = {
                                                "resourceType": "Condition",
                                                "meta": { "profile": [ "http://hl7.org/fhir/us/core/StructureDefinition/us-core-condition" ] },
                                                "verificationStatus": {
                                                                       "coding": [ {
                                                                                    "system": "http://terminology.hl7.org/CodeSystem/condition-ver-status"
                                                                                   } ]
                                                                      },
                                                "category": [ {
                                                               "coding": [ {
                                                                            "system": "http://terminology.hl7.org/CodeSystem/condition-category"
                                                                           } ]
                                                              } ],
                                                "code": {
                                                         "coding": [ { } ]
                                                        },
                                                "subject": { "reference": "Patient/" + hapi_pat_id },
                                                "identifier": []
                                            }
            
                            # Fill in the Condition resource template and send to HAPI
                            if api_call == "PUT":
                                cond_resource["id"] = cond["resource"]["id"]
                                total_dx_upd = total_dx_upd + 1
                            else:
                                total_dx_ins = total_dx_ins + 1
                            
                            if dx_vals[k][5] is not None:
                                cond_resource["recordedDate"] = dx_vals[k][5].strftime("%Y-%m-%d")
                            cond_resource["verificationStatus"]["coding"][0]["code"] = dx_to_verification_status[dx_vals[k][6]]
                            cond_resource["category"][0]["coding"][0]["code"] = dx_to_category[dx_vals[k][6]]
                            cond_resource["category"][0]["coding"][0]["display"] = category_code_to_display[dx_to_category[dx_vals[k][6]]]
                            cond_resource["code"]["coding"][0]["system"] = dx_to_coding_system(dx_vals[k][7])
                            cond_resource["code"]["coding"][0]["code"] = dx_to_coding_code(dx_vals[k][7])
                            cond_resource["code"]["coding"][0]["display"] = dx_to_coding_display(dx_vals[k][7])
                            cond_resource["code"]["text"] = dx_vals[k][7]
                            cond_resource["identifier"].append({
                                                                            "system": "https://cnics.cirg.washington.edu/diagnosis/site-record-id/" + pat_id_list[i][0].lower(),
                                                                            "value": str(dx_vals[k][4].decode("utf-8"))
                                                                           })

                            debug_logger.debug(orjson.dumps(cond_resource, option = orjson.OPT_NAIVE_UTC | orjson.OPT_INDENT_2).decode("utf-8"))
                                    
#                            headers = {"Content-Type": "application/fhir+json;charset=utf-8"}
                            if api_call == "PUT":
                                response = session.put(fhir_store_path + "/Condition/" + cond["resource"]["id"], headers = fhir_query_headers, json = cond_resource)
                            else:
                                response = session.post(fhir_store_path + "/Condition", headers = fhir_query_headers, json = cond_resource)
                            response.raise_for_status()
                            resource = response.json()
                            debug_logger.debug(resource)
        
                # If selected, collect current MedicationRequest resources for the patient
                if "medicationrequests" in resource_list:
                    response = session.get(fhir_store_path + "/MedicationRequest?subject=" + "Patient/" + hapi_pat_id + "&_format=json&_count=" + fhir_max_count, headers = fhir_query_headers)
                    response.raise_for_status()
                    reply = response.json()
                    debug_logger.debug(reply)
                    
                    if "entry" in reply:
                        med_entry_actions = [None] * len(reply["entry"])
                        
                        # Delete any existing MedicationRequest resources with no matching current medication
                        for l in range(0, len(reply["entry"])):
                            med = reply["entry"][l]
                            for k in range(0, len(med_vals)):
                                if str(med_vals[k][4].decode("utf-8")) == med["resource"]["identifier"][0]["value"]:
                                    med_entry_actions[l] = "update"
                                    break
                                else:
                                    med_entry_actions[l] = "delete"
                                    
                        for ind in range(0, len(med_entry_actions)):
                            if med_entry_actions[ind] == "delete":
                                response = session.delete(fhir_store_path + "/MedicationRequest/" + reply["entry"][ind]["resource"]["id"], headers = fhir_query_headers)
                                response.raise_for_status()
                                del_reply = response.json()
                                total_med_del = total_med_del + 1
                                debug_logger.debug(del_reply)
                    else:
                        med_entry_actions = []
                    
                    # Insert any new medications without existing MedicationRequest resource or update, if existing
                    for k in range(0, len(med_vals)):
                        if med_vals[k][3] != int(pat_id) and med_vals[k][5].strip() != '':
                            continue
                        else:
                            api_call = "POST"
                            if "entry" in reply:
                                for med in reply["entry"]:
                                    if str(med_vals[k][4].decode("utf-8")) == med["resource"]["identifier"][0]["value"]:
                                        api_call = "PUT"
                                        break
            
                            # Populate the bones of a MedicationRequest resource
                            med_resource = {
                                                "resourceType": "MedicationRequest",
                                                "meta": { "profile": [ "http://hl7.org/fhir/us/core/StructureDefinition/us-core-medicationrequest" ] },
                                                "intent": "order",
                                                "medicationCodeableConcept": {
                                                               "coding": [ {
                                                                            "system": "https://cnics.cirg.washington.edu/medication-name"
                                                                           } ]
                                                              },
                                                "subject": { "reference": "Patient/" + hapi_pat_id },
                                                "identifier": []
                                            }
            
                            # Fill in the MedicationRequest resource template and send to HAPI
                            if api_call == "PUT":
                                med_resource["id"] = med["resource"]["id"]
                                total_med_upd = total_med_upd + 1
                            else:
                                total_med_ins = total_med_ins + 1
                            
                            med_resource["status"] = med_to_status(med_vals[k][12], med_vals[k][13], med_vals[k][14])
                            med_resource["medicationCodeableConcept"]["coding"][0]["code"] = med_vals[k][5].replace("  ", " ")
                            med_resource["medicationCodeableConcept"]["coding"][0]["display"] = med_vals[k][5]
                            med_resource["medicationCodeableConcept"]["text"] = med_vals[k][5]
                            med_resource["identifier"].append({
                                                                            "system": "https://cnics.cirg.washington.edu/medication/site-record-id/" + pat_id_list[i][0].lower(),
                                                                            "value": str(med_vals[k][4].decode("utf-8"))
                                                                           })
                                
                            debug_logger.debug(orjson.dumps(med_resource, option = orjson.OPT_NAIVE_UTC | orjson.OPT_INDENT_2).decode("utf-8"))
                                    
#                            headers = {"Content-Type": "application/fhir+json;charset=utf-8"}
                            if api_call == "PUT":
                                response = session.put(fhir_store_path + "/MedicationRequest/" + med["resource"]["id"], headers = fhir_query_headers, json = med_resource)
                            else:
                                response = session.post(fhir_store_path + "/MedicationRequest", headers = fhir_query_headers, json = med_resource)
                            response.raise_for_status()
                            resource = response.json()
                            debug_logger.debug(resource)
        
                # If selected, collect current observation resources for the patient
                if "observations" in resource_list:
                    response = session.get(fhir_store_path + "/Observation?subject=" + "Patient/" + hapi_pat_id + "&_format=json&_count=" + fhir_max_count, headers = fhir_query_headers)
                    response.raise_for_status()
                    reply = response.json()
                    debug_logger.debug(reply)
                    
                    if "entry" in reply:
                        obs_entry_actions = [None] * len(reply["entry"])
                        
                        # Delete any existing observation resources with no matching current lab
                        for l in range(0, len(reply["entry"])):
                            obs = reply["entry"][l]
                            for k in range(0, len(lab_vals)):
                                if "identifier" in obs["resource"].keys():
                                    if lab_vals[k][4] == obs["resource"]["identifier"][0]["value"]:
                                        obs_entry_actions[l] = "update"
                                        break
                                    else:
                                        obs_entry_actions[l] = "delete"
                                    
                        for ind in range(0, len(obs_entry_actions)):
                            if obs_entry_actions[ind] == "delete":
                                response = session.delete(fhir_store_path + "/Observation/" + reply["entry"][ind]["resource"]["id"], headers = fhir_query_headers)
                                response.raise_for_status()
                                del_reply = response.json()
                                total_lab_del = total_lab_del + 1
                                debug_logger.debug(del_reply)
                    else:
                        obs_entry_actions = []
                    
                    # Insert any new labs without existing observation resource or update, if existing
                    for k in range(0, len(lab_vals)):
                        if lab_vals[k][3] != int(pat_id) and lab_vals[k][5].strip() != '':
                            continue
                        else:
                            api_call = "POST"
                            if "entry" in reply:
                                for obs in reply["entry"]:
                                    if "identifier" in obs["resource"].keys():
                                        if lab_vals[k][4] == obs["resource"]["identifier"][0]["value"]:
                                            api_call = "PUT"
                                            break
            
                            # Populate the bones of an observation resource
                            obs_resource = {
                                                "resourceType": "Observation",
                                                "meta": { "profile": [ "http://hl7.org/fhir/us/core/StructureDefinition/us-core-observation-lab" ] },
                                                "status": "final",
                                                "category": [ {
                                                               "coding": [ {
                                                                            "system": "http://terminology.hl7.org/CodeSystem/observation-category",
                                                                            "code": "laboratory",
                                                                            "display": "laboratory"
                                                                           } ]
                                                               } ],
                                                "code": {
                                                         "coding": [ { 
                                                                      "system": "https://cnics.cirg.washington.edu/test-name"
                                                                     } ]
                                                        },
                                                "subject": { "reference": "Patient/" + hapi_pat_id },
                                                "identifier": []
                                            }
            
                            # Fill in the Observation resource template and send to HAPI
                            if api_call == "PUT":
                                obs_resource["id"] = obs["resource"]["id"]
                                total_lab_upd = total_lab_upd + 1
                            else:
                                total_lab_ins = total_lab_ins + 1
                            
                            if lab_vals[k][9] is not None:
                                obs_resource["effectiveDateTime"] = lab_vals[k][9].strftime("%Y-%m-%d")
                            obs_resource["code"]["coding"][0]["code"] = lab_vals[k][5]
                            obs_resource["code"]["coding"][0]["display"] = lab_vals[k][5]
                            obs_resource["code"]["text"] = lab_vals[k][5]
                            
                            # Determine type of 'value[x]' to use based on the value of the lab result
                            value_val = lab_vals[k][6]
                            value_comparator = None
                            value_val_high = None
                            value_val_low = None
                            integer_re = '([0]|[-+]?\s*[1-9][0-9]*)'
                            decimal_re = '(-?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][+-]?[0-9]+)?)'
                            range_re = '([0]|[-+]?\s*[1-9][0-9]*)\s*-\s*([0]|[-+]?\s*[1-9][0-9]*)'
                            comparator_re = '(<|<=|>=|>)'
                            # if integer, use 'valueInteger'
                            if re.search("^" + integer_re + "$", value_val) != None:
                                value_type = "valueInteger"
                                value_val = int(value_val)
                            # if range, use 'valueRange' with 'high' and 'low' elements
                            elif re.search("^" + range_re + "$", value_val) != None:
                                value_type = "valueRange"
                                value_val_high = re.search("^" + range_re + "$", value_val).groups()[1]
                                value_val_low = re.search("^" + range_re + "$", value_val).groups()[0]
                            # if decimal, use 'valueQuantity'
                            elif re.search("^" + decimal_re + "$", value_val) != None:
                                value_type = "valueQuantity"
                                value_val = float(value_val)
                            # if there's a comaprator prior to the decimal, use 'valueQuantity' and add in a 'valueComparator' element
                            elif re.search("^" + comparator_re + decimal_re + "$", value_val) != None:
                                value_type = "valueQuantity"
                                value_comparator = re.search("^" + comparator_re + decimal_re + "$", value_val).groups()[0]
                                value_val = float(re.search("^" + comparator_re + decimal_re + "$", value_val).groups()[1])
                            else:
                                value_type = "valueString"
        
                            if value_type in ["valueRange", "valueQuantity"]:
                                obs_resource[value_type] = {}
                                if value_type == "valueRange":
                                    obs_resource[value_type]["low"] = {}
                                    obs_resource[value_type]["low"]["value"] = float(value_val_low)
                                    obs_resource[value_type]["high"] = {}
                                    obs_resource[value_type]["high"]["value"] = float(value_val_high)
                                else:
                                    obs_resource[value_type]["value"] = value_val
                                    # set some defaults here in case there is no unit value supplied, if there is it will be overwritten below
                                    obs_resource[value_type]["unit"] = '%'
                                    obs_resource[value_type]["system"] = "http://unitsofmeasure.org"
                                    obs_resource[value_type]["code"] = '%'
                                if value_comparator != None:
                                    obs_resource[value_type]["comparator"] = value_comparator
                                if lab_vals[k][7] != None:
                                    if value_type == "valueRange":
                                        obs_resource[value_type]["low"]["unit"] = lab_vals[k][7]
                                        obs_resource[value_type]["low"]["system"] = "http://unitsofmeasure.org"
                                        obs_resource[value_type]["low"]["code"] = lab_vals[k][7]
                                        obs_resource[value_type]["high"]["unit"] = lab_vals[k][7]
                                        obs_resource[value_type]["high"]["system"] = "http://unitsofmeasure.org"
                                        obs_resource[value_type]["high"]["code"] = lab_vals[k][7]
                                    else :
                                        obs_resource[value_type]["unit"] = lab_vals[k][7]
                                        obs_resource[value_type]["system"] = "http://unitsofmeasure.org"
                                        obs_resource[value_type]["code"] = lab_vals[k][7]
                                if lab_vals[k][10] != None or lab_vals[k][11] != None:
                                    if re.search("^" + decimal_re + "$", str(lab_vals[k][10])) != None or re.search("^" + decimal_re + "$", str(lab_vals[k][11])) != None:
                                        obs_resource["referenceRange"] = [ {
                                                                                        "type" : {
                                                                                                  "coding" : [ {
                                                                                                                "system" : "http://terminology.hl7.org/CodeSystem/referencerange-meaning",
                                                                                                                "code" : "normal",
                                                                                                                "display" : "Normal Range"
                                                                                                               }
                                                                                                             ],
                                                                                                  "text" : "Normal Range"
                                                                                                 }
                
                                                                                       } ]
                                        if re.search("^" + decimal_re + "$", str(lab_vals[k][10])) != None:
                                            obs_resource["referenceRange"][0]["low"] = {
                                                                                                    "value" : float(lab_vals[k][10])
                                                                                                   }
                                            if lab_vals[k][7] != None:
                                                obs_resource["referenceRange"][0]["low"]["unit"] = lab_vals[k][7]
                                                obs_resource["referenceRange"][0]["low"]["system"] = "http://unitsofmeasure.org"
                                                obs_resource["referenceRange"][0]["low"]["code"] = lab_vals[k][7]
                
                                        if re.search("^" + decimal_re + "$", str(lab_vals[k][11])) != None:
                                            obs_resource["referenceRange"][0]["high"] = {
                                                                                                    "value" : float(lab_vals[k][11])
                                                                                                    }
                                            if lab_vals[k][7] != None:
                                                obs_resource["referenceRange"][0]["high"]["unit"] = lab_vals[k][7]
                                                obs_resource["referenceRange"][0]["high"]["system"] = "http://unitsofmeasure.org"
                                                obs_resource["referenceRange"][0]["high"]["code"] = lab_vals[k][7]
                            else:
                                obs_resource[value_type] = value_val
        
                            obs_resource["identifier"].append({
                                                                  "system": "https://cnics.cirg.washington.edu/lab/site-record-id/" + pat_id_list[i][0].lower(),
                                                                  "value": lab_vals[k][4]
                                                              })
                                
                            debug_logger.debug(orjson.dumps(obs_resource, option = orjson.OPT_NAIVE_UTC | orjson.OPT_INDENT_2).decode("utf-8"))
                                    
#                            headers = {"Content-Type": "application/fhir+json;charset=utf-8"}
                            if api_call == "PUT":
                                response = session.put(fhir_store_path + "/Observation/" + obs["resource"]["id"], headers = fhir_query_headers, json = obs_resource)
                            else:
                                response = session.post(fhir_store_path + "/Observation", headers = fhir_query_headers, json = obs_resource)
                            response.raise_for_status()
                            resource = response.json()
                            debug_logger.debug(resource)
        
            else:
                # Multiple patient resources found, halt and catch fire!
                info_logger.error("ERROR: Multiple patient resources (" + str(reply["total"]) + ") found with the same CNICS ID: "  + pat_id_list[i][0].lower() + "|" + str(pat_vals[0][1].decode("utf-8")) + ".  This should never happen, aborting.")
        
        # cnxn.close()
        
        bench_end = datetime.datetime.now()
        
        info_logger.info("Total Patients Deleted: " + str(total_pat_del))
        info_logger.info("Total Patients Inserted: " + str(total_pat_ins))
        info_logger.info("Total Patients Updated: " + str(total_pat_upd))
        info_logger.info("Total Conditions Deleted: " + str(total_dx_del))
        info_logger.info("Total Conditions Inserted: " + str(total_dx_ins))
        info_logger.info("Total Conditions Updated: " + str(total_dx_upd))
        info_logger.info("Total Medications Deleted: " + str(total_med_del))
        info_logger.info("Total Medications Inserted: " + str(total_med_ins))
        info_logger.info("Total Medications Updated: " + str(total_med_upd))
        info_logger.info("Total Observations Deleted: " + str(total_lab_del))
        info_logger.info("Total Observations Inserted: " + str(total_lab_ins))
        info_logger.info("Total Observations Updated: " + str(total_lab_upd))
        info_logger.info("Total Run Time: " + str(bench_end - bench_start))

    job_cnt += 1
