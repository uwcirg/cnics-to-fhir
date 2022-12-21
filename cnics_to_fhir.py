# -*- coding: utf-8 -*-
"""
Created on Thu Jul 22 13:01:49 2021

@author: James Sibley
"""

import configparser, csv, datetime, mysql.connector, orjson, re, requests
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

def log_it(message, email_notify = False):
    LOG_FILE.write(message + "\n")
    if email_notify:
        notify({'mail_date': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S %z"),
                'mail_subj': "CNICS to FHIR Notification",
                'mail_body': message + "\n"})

def med_to_status(start_date, end_date, end_type):
    if start_date is not None:
        if end_date is not None:
            return "stopped"
        return "active"

def notify(post_data):
    try:
        response = requests.post('https://jumpstart2.cirg.washington.edu/cgi-bin/notify-sms.cgi', data = post_data, verify = False)
    except ConnectionError as e:
        log_it('ERR: Notify failed - ConnectionError (' + str(e) + ')...')
    except HTTPError as e:
        log_it('ERR: Notify failed - HTTPError (' + str(e) + ')...')
    except Timeout as e:
        log_it('ERR: Notify failed - Timeout (' + str(e) + ')...')
    except Exception as e:
        log_it('ERR: Notify failed (' + str(e) + ')...')
    else:
        if response is not None:
            log_it('Notify completed, status code (' + str(response.status_code) + ')...')

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
"""
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
"""
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
"""

def pro_sql_gen(obj_type, session_id):
    if obj_type == 5:
#        print("select * from ProAltered where PatientId = '" + pat_id + "' order by ProId")
        return """
select distinct p.PatientID, p.MRN
from Patients p
join Sessions s on p.PatientID = s.PatientID
where s.SessionID = '""" + session_id + """'
"""

SETTINGS = configparser.ConfigParser()
SETTINGS.read('./settings.ini')
SECRETS = configparser.ConfigParser()
SECRETS.read('./secrets.ini')

LOG_FILE = open(SETTINGS['Logging']['LogPath'].strip('"') + "cnics_to_fhir.log", "a", encoding="utf-8")
LOG_LEVEL = SETTINGS['Logging']['LogLevel'].strip('"')

with open(SETTINGS['Files']['StandardDiagnoses'].strip('"')) as f:
    CNICS_STANDARD_DIAGNOSES = [i.replace('"', '') for i in f.read().splitlines()]
with open(SETTINGS['Files']['StandardMedications'].strip('"')) as f:
    CNICS_STANDARD_MEDICATIONS = [i.replace('"', '') for i in f.read().splitlines()]

cnxn = mysql.connector.connect(user = SETTINGS['Database']['DataUser'].strip('"'),
                               password = SECRETS['Database']['DataPw'].strip('"'),
                               host = SETTINGS['Database']['DataHost'].strip('"'),
                               port = SETTINGS['Database']['DataPort'].strip('"'),
                               database = SETTINGS['Database']['DataDb'].strip('"'))
# "Reveal" database connection to pull MRN identifiers from PRO database
pro_cnxn = mysql.connector.connect(user = SETTINGS['Database']['ProUser'].strip('"'),
                                   password = SECRETS['Database']['ProPw'].strip('"'),
                                   host = SETTINGS['Database']['ProHost'].strip('"'),
                                   port = SETTINGS['Database']['ProPort'].strip('"'),
                                   database = SETTINGS['Database']['ProDb'].strip('"'))
pro_cursor = pro_cnxn.cursor()

fhir_store_path = SETTINGS['Options']['FhirUrl'].strip('"')
# Set a maximum number of resources to return in FHIR queries
# Note this is a temporary hack, paginating results should be implemented instead
fhir_max_count = "50000"
pat_id_list = []

# Which types of resources should we include in this ETL run, read from settings file
resource_list = SETTINGS['Options']['ResourceList'].strip('"').split(",")

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
# Build list of Site:SitePatientId pairs to query 
cursor = cnxn.cursor()
cursor.execute("""
select *
from Patient p
join DemographicAltered d on p.PatientId = d.PatientId
where Site in (""" + SETTINGS['Options']['SiteList'].strip('"') + """)
#order by rand()
limit """ + SETTINGS['Options']['PatCnt'].strip('"') + """
""")
pat_vals = cursor.fetchall()

pat_id_fn = SETTINGS['Logging']['LogPath'].strip('"') + "cnics_to_fhir_pid_list.txt"
pat_id_file = open(pat_id_fn, "w", encoding="utf-8")
for i in range(0, len(pat_vals)):
  pat_id_file.write(str(pat_vals[i][2]) + ':' + pat_vals[i][1].decode("utf-8").replace("'", "''") + '\n')
pat_id_file.close()

with open(pat_id_fn) as pat_id_file:
    pat_id_lines = [line.rstrip() for line in pat_id_file]

log_it("==================================================================")
log_it("Run Date/Time: " + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
log_it("FHIR URL: " + SETTINGS['Options']['FhirUrl'].strip('"'))
log_it("Resource List: " + SETTINGS['Options']['ResourceList'].strip('"'))
log_it("Site List: " + SETTINGS['Options']['SiteList'].strip('"'))
log_it("Patient Count: " + str(len(pat_id_lines)))

for i in range(0, len(pat_id_lines)):
    pat_id_list.append(tuple([str(pat_id_lines[i].split(":")[0]), pat_id_lines[i].split(":")[1]]))

# Read in MRNs from an external file to use as additional identifiers for patient resources
# This is currently very specific for the "UW" site, will need modifications to accommodate other sites
site_id_mrns = {}
if SETTINGS['Options']['SiteList'].strip('"').strip("'") == 'UW':
    cnt = 0
    mrn_file = SETTINGS['Files']['Mrns' + SETTINGS['Options']['SiteList'].strip('"').strip("'")].strip('"')
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

# Query for patient data, xform to FHIR bundle, upload to HAPI as insert (if new) or update (if existing)
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
response = requests.get(fhir_store_path + "/Patient?identifier=https://cnics.cirg.washington.edu/site-patient-id/" + SETTINGS['Options']['SiteList'].strip('"').strip("'").lower() + "|&_format=json&_count=" + fhir_max_count)
response.raise_for_status()
reply = response.json()
if int(LOG_LEVEL) > 8:
    print("=====")
    print(reply)

if "entry" in reply:
    # Delete any existing patients with no matching current entry
    for l in range(0, len(reply["entry"])):
        pat = reply["entry"][l]
        if pat["resource"]["identifier"][0]["value"] not in [x[1] for x in pat_id_list]:
            response = requests.delete(fhir_store_path + "/Patient/" + pat["resource"]["id"] + "?_cascade=delete")
            response.raise_for_status()
            del_reply = response.json()
            total_pat_del = total_pat_del + 1
            if int(LOG_LEVEL) > 8:
                print("=====")
                print(del_reply)

for i in range(0, len(pat_id_list)):
    final_pat_bundle = {
                        "resourceType": "Bundle",
                        "type": "transaction",
                        "entry": []
                       }
    
    cursor.execute(sql_gen(0, None, pat_id_list[i][0], pat_id_list[i][1]))
    pat_vals = cursor.fetchall()
    pat_id = str(pat_vals[0][0])
    
    cursor.execute(sql_gen(1, pat_id, None, None))
    dx_vals = cursor.fetchall()
    
    cursor.execute(sql_gen(2, pat_id, None, None))
    demo_vals = cursor.fetchall()

    cursor.execute(sql_gen(3, pat_id, None, None))
    med_vals = cursor.fetchall()

    cursor.execute(sql_gen(4, pat_id, None, None))
    sess_vals = cursor.fetchall()

    cursor.execute(sql_gen(5, pat_id, None, None))
    lab_vals = cursor.fetchall()

    # See if patient resource already exists, get ID if yes
    response = requests.get(fhir_store_path + "/Patient?identifier=https://cnics.cirg.washington.edu/site-patient-id/" + pat_id_list[i][0].lower() + "|" + str(pat_vals[0][1].decode("utf-8")) + "&_format=json&_count=" + fhir_max_count)
    response.raise_for_status()
    reply = response.json()
    if int(LOG_LEVEL) > 8:
        print("=====")
        print(reply)

    if reply["total"] < 2:
        # Insert or update
        if reply["total"] == 1:
            hapi_pat_id = reply["entry"][0]["resource"]["id"]
        else:
            hapi_pat_id = None

        # Populate the bones of a patient resource
        pat_resource = {
                        "resource": {
                                     "resourceType": "Patient",
                                     "meta": { "profile": ["http://hl7.org/fhir/us/core/StructureDefinition/us-core-patient"] },
                                     "text": {
                                              "status": "generated",
                                              "div": "<div xmlns=\"http://www.w3.org/1999/xhtml\">Generated by CIRG's CNICS to FHIR. Version identifier: 0.1</div>"
                                             },
                                     "extension": [],
                                     "identifier": []
                                    },
                        "request": {
                                    "url": "Patient"
                                   }
                        
                       }
        
        if hapi_pat_id is not None:
            pat_resource["resource"]["id"] = hapi_pat_id
            pat_resource["request"]["method"] = "PUT"
            total_pat_upd = total_pat_upd + 1
        else:
            pat_resource["request"]["method"] = "POST"
            total_pat_ins = total_pat_ins + 1
            
        # Fill in identifiers, if any
        if pat_vals[0][1] is not None:
            pat_resource["resource"]["identifier"].append({
                                                           "system": "https://cnics.cirg.washington.edu/site-patient-id/" + pat_id_list[i][0].lower(),
                                                           "value": str(pat_vals[0][1].decode("utf-8"))
                                                          })
        # All SessionId values from the ProAltered table to make it easier to match patients with the PRO system
        for j in range(0, len(sess_vals)):
            pat_resource["resource"]["identifier"].append({
                                                           "system": "https://cnics-pro.cirg.washington.edu/session-id/" + pat_id_list[i][0].lower(),
                                                           "value": sess_vals[j][0]
                                                          })
        
        # MRNs from the local clinic site, if provided in a separate file
        if str(pat_vals[0][1].decode("utf-8")) in site_id_mrns.keys():
            if 'hmrn' in site_id_mrns[str(pat_vals[0][1].decode("utf-8"))].keys():
                pat_resource["resource"]["identifier"].append({
                                                               "system": "https://cnics-pro.cirg.washington.edu/institution-mrn/" + pat_id_list[i][0].lower(),
                                                               "value": site_id_mrns[str(pat_vals[0][1].decode("utf-8"))]['hmrn']
                                                              })
            if 'umrn' in site_id_mrns[str(pat_vals[0][1].decode("utf-8"))].keys():
                pat_resource["resource"]["identifier"].append({
                                                               "system": "https://cnics-pro.cirg.washington.edu/institution-mrn/" + pat_id_list[i][0].lower(),
                                                               "value": site_id_mrns[str(pat_vals[0][1].decode("utf-8"))]['umrn']
                                                              })
        else: # Get MRN and PRO PatientID values from the PRO system
            uniq_pro_pat_ids = []
            uniq_pro_mrns = []
            for j in range(0, len(sess_vals)):
                pro_cursor.execute(pro_sql_gen(5, sess_vals[j][0]))
                id_vals = pro_cursor.fetchall()
                for k in range(0, len(id_vals)):
                    if id_vals[k][0] is not None:
                        if id_vals[k][0] not in uniq_pro_pat_ids:
                            uniq_pro_pat_ids.append(id_vals[k][0])
                    if id_vals[k][1] is not None:
                        if id_vals[k][1] not in uniq_pro_mrns:
                            uniq_pro_mrns.append(id_vals[k][1])

            for uniq_id in uniq_pro_pat_ids:
                pat_resource["resource"]["identifier"].append({
                                                               "system": "https://cnics-pro.cirg.washington.edu/pro-patient-id/" + pat_id_list[i][0].lower(),
                                                               "value": uniq_id
                                                              })
            for uniq_id in uniq_pro_mrns:
                pat_resource["resource"]["identifier"].append({
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
                        pat_resource["resource"]["extension"].append(pat_race)
    
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
                        pat_resource["resource"]["extension"].append(pat_eth)
    
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
                        pat_resource["resource"]["extension"].append(pat_sex)
                        pat_resource["resource"]["gender"] = demo_vals[j][6].lower()
    
                break
    
        final_pat_bundle["entry"].append(pat_resource)

        if int(LOG_LEVEL) > 8:
            print(orjson.dumps(final_pat_bundle, option = orjson.OPT_NAIVE_UTC | orjson.OPT_INDENT_2).decode("utf-8"))
                
        headers = {"Content-Type": "application/fhir+json;charset=utf-8"}
        if hapi_pat_id is not None:
            response = requests.put(fhir_store_path + "/Patient/" + hapi_pat_id, headers = headers, json = final_pat_bundle["entry"][0]["resource"])
        else:
            response = requests.post(fhir_store_path, headers = headers, json = final_pat_bundle)
        response.raise_for_status()
        resource = response.json()
        if int(LOG_LEVEL) > 8:
            print(resource)

        if hapi_pat_id is None:
            hapi_pat_id = resource["entry"][0]["response"]["location"].split("/")[1]
        
        # If selected, collect current condition resources for the patient
        if "conditions" in resource_list:
            response = requests.get(fhir_store_path + "/Condition?subject=" + "Patient/" + hapi_pat_id + "&_format=json&_count=" + fhir_max_count)
            response.raise_for_status()
            reply = response.json()
            if int(LOG_LEVEL) > 8:
                print("=====")
                print(reply)
            
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
                        response = requests.delete(fhir_store_path + "/Condition/" + reply["entry"][ind]["resource"]["id"])
                        response.raise_for_status()
                        del_reply = response.json()
                        total_dx_del = total_dx_del + 1
                        if int(LOG_LEVEL) > 8:
                            print("=====")
                            print(del_reply)
            else:
                cond_entry_actions = []
            
            # Insert any new diagnoses without existing condition resource or update, if existing
            for k in range(0, len(dx_vals)):
                final_dx_bundle = {
                                   "resourceType": "Bundle",
                                   "type": "transaction",
                                   "entry": []
                                  }
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
                                     "resource": {
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
                                                 },
                                     "request": {
                                                 "url": "Condition"
                                                }
                                    }
    
                    # Fill in the Condition resource template and send to HAPI
                    if api_call == "PUT":
                        cond_resource["resource"]["id"] = cond["resource"]["id"]
                        total_dx_upd = total_dx_upd + 1
                    else:
                        total_dx_ins = total_dx_ins + 1
                    
                    if dx_vals[k][5] is not None:
                        cond_resource["resource"]["recordedDate"] = dx_vals[k][5].strftime("%Y-%m-%d")
                    cond_resource["resource"]["verificationStatus"]["coding"][0]["code"] = dx_to_verification_status[dx_vals[k][6]]
                    cond_resource["resource"]["category"][0]["coding"][0]["code"] = dx_to_category[dx_vals[k][6]]
                    cond_resource["resource"]["category"][0]["coding"][0]["display"] = category_code_to_display[dx_to_category[dx_vals[k][6]]]
                    cond_resource["resource"]["code"]["coding"][0]["system"] = dx_to_coding_system(dx_vals[k][7])
                    cond_resource["resource"]["code"]["coding"][0]["code"] = dx_to_coding_code(dx_vals[k][7])
                    cond_resource["resource"]["code"]["coding"][0]["display"] = dx_to_coding_display(dx_vals[k][7])
                    cond_resource["resource"]["code"]["text"] = dx_vals[k][7]
                    cond_resource["resource"]["identifier"].append({
                                                                    "system": "https://cnics.cirg.washington.edu/diagnosis/site-record-id/" + pat_id_list[i][0].lower(),
                                                                    "value": str(dx_vals[k][4].decode("utf-8"))
                                                                   })
                    cond_resource["request"]["method"] = api_call
    
                    final_dx_bundle["entry"].append(cond_resource)
            
                    if int(LOG_LEVEL) > 8:
                        print(orjson.dumps(final_dx_bundle, option = orjson.OPT_NAIVE_UTC | orjson.OPT_INDENT_2).decode("utf-8"))
                            
                    headers = {"Content-Type": "application/fhir+json;charset=utf-8"}
                    if api_call == "PUT":
                        response = requests.put(fhir_store_path + "/Condition/" + cond["resource"]["id"], headers = headers, json = final_dx_bundle["entry"][0]["resource"])
                    else:
                        response = requests.post(fhir_store_path, headers = headers, json = final_dx_bundle)
                    response.raise_for_status()
                    resource = response.json()
                    if int(LOG_LEVEL) > 8:
                        print(resource)

        # If selected, collect current MedicationRequest resources for the patient
        if "medicationrequests" in resource_list:
            response = requests.get(fhir_store_path + "/MedicationRequest?subject=" + "Patient/" + hapi_pat_id + "&_format=json&_count=" + fhir_max_count)
            response.raise_for_status()
            reply = response.json()
            if int(LOG_LEVEL) > 8:
                print("=====")
                print(reply)
            
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
                        response = requests.delete(fhir_store_path + "/MedicationRequest/" + reply["entry"][ind]["resource"]["id"])
                        response.raise_for_status()
                        del_reply = response.json()
                        total_med_del = total_med_del + 1
                        if int(LOG_LEVEL) > 8:
                            print("=====")
                            print(del_reply)
            else:
                med_entry_actions = []
            
            # Insert any new medications without existing MedicationRequest resource or update, if existing
            for k in range(0, len(med_vals)):
                final_med_bundle = {
                                   "resourceType": "Bundle",
                                   "type": "transaction",
                                   "entry": []
                                  }
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
                                     "resource": {
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
                                                 },
                                     "request": {
                                                 "url": "MedicationRequest"
                                                }
                                    }
    
                    # Fill in the MedicationRequest resource template and send to HAPI
                    if api_call == "PUT":
                        med_resource["resource"]["id"] = med["resource"]["id"]
                        total_med_upd = total_med_upd + 1
                    else:
                        total_med_ins = total_med_ins + 1
                    
                    med_resource["resource"]["status"] = med_to_status(med_vals[k][12], med_vals[k][13], med_vals[k][14])
                    med_resource["resource"]["medicationCodeableConcept"]["coding"][0]["code"] = med_vals[k][5]
                    med_resource["resource"]["medicationCodeableConcept"]["coding"][0]["display"] = med_vals[k][5]
                    med_resource["resource"]["medicationCodeableConcept"]["text"] = med_vals[k][5]
                    med_resource["resource"]["identifier"].append({
                                                                    "system": "https://cnics.cirg.washington.edu/medication/site-record-id/" + pat_id_list[i][0].lower(),
                                                                    "value": str(med_vals[k][4].decode("utf-8"))
                                                                   })
                    med_resource["request"]["method"] = api_call
    
                    final_med_bundle["entry"].append(med_resource)
            
                    if int(LOG_LEVEL) > 8:
                        print(orjson.dumps(final_med_bundle, option = orjson.OPT_NAIVE_UTC | orjson.OPT_INDENT_2).decode("utf-8"))
                            
                    headers = {"Content-Type": "application/fhir+json;charset=utf-8"}
                    if api_call == "PUT":
                        response = requests.put(fhir_store_path + "/MedicationRequest/" + med["resource"]["id"], headers = headers, json = final_med_bundle["entry"][0]["resource"])
                    else:
                        response = requests.post(fhir_store_path, headers = headers, json = final_med_bundle)
                    response.raise_for_status()
                    resource = response.json()
                    if int(LOG_LEVEL) > 8:
                        print(resource)

        # If selected, collect current observation resources for the patient
        if "observations" in resource_list:
            response = requests.get(fhir_store_path + "/Observation?subject=" + "Patient/" + hapi_pat_id + "&_format=json&_count=" + fhir_max_count)
            response.raise_for_status()
            reply = response.json()
            if int(LOG_LEVEL) > 8:
                print("=====")
                print(reply)
            
            if "entry" in reply:
                obs_entry_actions = [None] * len(reply["entry"])
                
                # Delete any existing observation resources with no matching current lab
                for l in range(0, len(reply["entry"])):
                    obs = reply["entry"][l]
                    for k in range(0, len(lab_vals)):
                        if "identifier" in obs["resource"].keys():
                            if str(lab_vals[k][4].decode("utf-8")) == obs["resource"]["identifier"][0]["value"]:
                                obs_entry_actions[l] = "update"
                                break
                            else:
                                obs_entry_actions[l] = "delete"
                            
                for ind in range(0, len(obs_entry_actions)):
                    if obs_entry_actions[ind] == "delete":
                        response = requests.delete(fhir_store_path + "/Observation/" + reply["entry"][ind]["resource"]["id"])
                        response.raise_for_status()
                        del_reply = response.json()
                        total_lab_del = total_lab_del + 1
                        if int(LOG_LEVEL) > 8:
                            print("=====")
                            print(del_reply)
            else:
                obs_entry_actions = []
            
            # Insert any new labs without existing observation resource or update, if existing
            for k in range(0, len(lab_vals)):
                final_lab_bundle = {
                                    "resourceType": "Bundle",
                                    "type": "transaction",
                                    "entry": []
                                   }
                if lab_vals[k][3] != int(pat_id) and lab_vals[k][5].strip() != '':
                    continue
                else:
                    api_call = "POST"
                    if "entry" in reply:
                        for obs in reply["entry"]:
                            if "identifier" in obs["resource"].keys():
                                if str(lab_vals[k][4].decode("utf-8")) == obs["resource"]["identifier"][0]["value"]:
                                    api_call = "PUT"
                                    break
    
                    # Populate the bones of an observation resource
                    obs_resource = {
                                     "resource": {
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
                                                 },
                                     "request": {
                                                 "url": "Observation"
                                                }
                                    }
    
                    # Fill in the Observation resource template and send to HAPI
                    if api_call == "PUT":
                        obs_resource["resource"]["id"] = obs["resource"]["id"]
                        total_lab_upd = total_lab_upd + 1
                    else:
                        total_lab_ins = total_lab_ins + 1
                    
                    if lab_vals[k][9] is not None:
                        obs_resource["resource"]["effectiveDateTime"] = lab_vals[k][9].strftime("%Y-%m-%d")
                    obs_resource["resource"]["code"]["coding"][0]["code"] = lab_vals[k][5]
                    obs_resource["resource"]["code"]["coding"][0]["display"] = lab_vals[k][5]
                    obs_resource["resource"]["code"]["text"] = lab_vals[k][5]
                    
                    # Determine type of 'value[x]' to use based on the value of the lab result
                    value_val = lab_vals[k][6]
                    value_comparator = None
                    value_val_high = None
                    value_val_low = None
                    integer_re = '([0]|[-+]?\s*[1-9][0-9]*)'
                    decimal_re = '-?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][+-]?[0-9]+)?'
                    range_re = '([0]|[-+]?\s*[1-9][0-9]*)\s*-\s*([0]|[-+]?\s*[1-9][0-9]*)'
                    comparator_re = '(<|<=|>=|>)'
                    # if integer, use 'valueInteger'
                    if re.search("^" + integer_re + "$", value_val) != None:
                        value_type = "valueInteger"
                    # if range, use 'valueRange' with 'high' and 'low' elements
                    elif re.search("^" + range_re + "$", value_val) != None:
                        value_type = "valueRange"
                        value_val_high = re.search("^" + range_re + "$", value_val).groups()[1]
                        value_val_low = re.search("^" + range_re + "$", value_val).groups()[0]
                    # if decimal, use 'valueQuantity'
                    elif re.search("^" + decimal_re + "$", value_val) != None:
                        value_type = "valueQuantity"
                    # if there's a comaprator prior to the decimal, use 'valueQuantity' and add in a 'valueComparator' element
                    elif re.search("^" + comparator_re + decimal_re + "$", value_val) != None:
                        value_type = "valueQuantity"
                        value_comparator = re.search("^" + comparator_re + decimal_re + "$", value_val).groups()[0]
                        value_val = "".join(re.search("^" + comparator_re + decimal_re + "$", value_val).groups()[1:])
                    else:
                        value_type = "valueString"
                        value_val = "\"" + value_val + "\""

                    obs_resource["resource"][value_type] = {}
                    if value_type == "valueRange":
                        obs_resource["resource"][value_type]["low"] = {}
                        obs_resource["resource"][value_type]["low"]["value"] = value_val_low
                        obs_resource["resource"][value_type]["high"] = {}
                        obs_resource["resource"][value_type]["high"]["value"] = value_val_high
                    else:
                        obs_resource["resource"][value_type]["value"] = value_val
                    if value_comparator != None:
                        obs_resource["resource"][value_type]["comparator"] = value_comparator
                    if lab_vals[k][7] != None:
                        if value_type == "valueRange":
                            obs_resource["resource"][value_type]["low"]["unit"] = lab_vals[k][7]
                            obs_resource["resource"][value_type]["low"]["system"] = "http://unitsofmeasure.org"
                            obs_resource["resource"][value_type]["low"]["code"] = lab_vals[k][7]
                            obs_resource["resource"][value_type]["high"]["unit"] = lab_vals[k][7]
                            obs_resource["resource"][value_type]["high"]["system"] = "http://unitsofmeasure.org"
                            obs_resource["resource"][value_type]["high"]["code"] = lab_vals[k][7]
                        else :
                            obs_resource["resource"][value_type]["unit"] = lab_vals[k][7]
                            obs_resource["resource"][value_type]["system"] = "http://unitsofmeasure.org"
                            obs_resource["resource"][value_type]["code"] = lab_vals[k][7]
                    if lab_vals[k][10] != None or lab_vals[k][11] != None:
                        obs_resource["resource"]["referenceRange"] = [ {
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
                        if lab_vals[k][10] is not None:
                            obs_resource["resource"]["referenceRange"][0]["low"] = {
                                                                                    "value" : lab_vals[k][10]
                                                                                   }
                            if lab_vals[k][7] != None:
                                obs_resource["resource"]["referenceRange"][0]["low"]["unit"] = lab_vals[k][7]
                                obs_resource["resource"]["referenceRange"][0]["low"]["system"] = "http://unitsofmeasure.org"
                                obs_resource["resource"]["referenceRange"][0]["low"]["code"] = lab_vals[k][7]

                        if lab_vals[k][11] is not None:
                            obs_resource["resource"]["referenceRange"][0]["high"] = {
                                                                                    "value" : lab_vals[k][11]
                                                                                    }
                            if lab_vals[k][7] != None:
                                obs_resource["resource"]["referenceRange"][0]["high"]["unit"] = lab_vals[k][7]
                                obs_resource["resource"]["referenceRange"][0]["high"]["system"] = "http://unitsofmeasure.org"
                                obs_resource["resource"]["referenceRange"][0]["high"]["code"] = lab_vals[k][7]

                    obs_resource["resource"]["identifier"].append({
                                                                    "system": "https://cnics.cirg.washington.edu/lab/site-record-id/" + pat_id_list[i][0].lower(),
                                                                    "value": str(lab_vals[k][4].decode("utf-8"))
                                                                  })
                    obs_resource["request"]["method"] = api_call
    
                    final_lab_bundle["entry"].append(obs_resource)
            
                    if int(LOG_LEVEL) > 8:
                        print(orjson.dumps(final_lab_bundle, option = orjson.OPT_NAIVE_UTC | orjson.OPT_INDENT_2).decode("utf-8"))
                            
                    headers = {"Content-Type": "application/fhir+json;charset=utf-8"}
                    if api_call == "PUT":
                        response = requests.put(fhir_store_path + "/Observation/" + obs["resource"]["id"], headers = headers, json = final_lab_bundle["entry"][0]["resource"])
                    else:
                        response = requests.post(fhir_store_path, headers = headers, json = final_lab_bundle)
                    response.raise_for_status()
                    resource = response.json()
                    if int(LOG_LEVEL) > 8:
                        print(resource)

    else:
        # Multiple patient resources found, halt and catch fire!
        log_it("ERROR: Multiple patient resources (" + str(reply["total"]) + ") found with the same CNICS ID: "  + pat_id_list[i][0].lower() + "|" + str(pat_vals[0][1].decode("utf-8")) + ".  This should never happen, aborting.")

cnxn.close()

bench_end = datetime.datetime.now()

log_it("Total Patients Deleted: " + str(total_pat_del))
log_it("Total Patients Inserted: " + str(total_pat_ins))
log_it("Total Patients Updated: " + str(total_pat_upd))
log_it("Total Conditions Deleted: " + str(total_dx_del))
log_it("Total Conditions Inserted: " + str(total_dx_ins))
log_it("Total Conditions Updated: " + str(total_dx_upd))
log_it("Total Medications Deleted: " + str(total_med_del))
log_it("Total Medications Inserted: " + str(total_med_ins))
log_it("Total Medications Updated: " + str(total_med_upd))
log_it("Total Observations Deleted: " + str(total_lab_del))
log_it("Total Observations Inserted: " + str(total_lab_ins))
log_it("Total Observations Updated: " + str(total_lab_upd))
log_it("Total Run Time: " + str(bench_end - bench_start))

LOG_FILE.close()
