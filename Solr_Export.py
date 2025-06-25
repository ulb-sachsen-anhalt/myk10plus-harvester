import json
import sys
import os
import requests
import datetime
import traceback




if len(sys.argv) > 1:
    Settingsfile = sys.argv[1]
else:
    Settingsfile = "Settings/Settings.json"

try:
    with open(Settingsfile, "r") as file:
        Settings = json.load(file)
except Exception as E:
    print("Error loading Settingsfile:")
    print(str(E))
    sys.exit()

try:
    New_Folder = Settings["New_Folder"]
    Delete_Folder = Settings["Delete_Folder"]
    Logs_Folder = Settings["Logs_Folder"]
    SOLR_Base = Settings["SOLR_Base"]
    Logname = "_Solr/"
    In_MyK10 = Settings["InMyK10_Folder"]
    Del_MyK10 = Settings["MyK10_Deleted_Folder"]
    if not os.path.isdir(In_MyK10):
        os.makedirs(In_MyK10)
    if not os.path.isdir(Del_MyK10):
        os.makedirs(Del_MyK10)
    Today = datetime.datetime.strftime(datetime.datetime.today(), '%Y-%m-%d_%H-%M-%S')
    if not os.path.isdir(Logs_Folder + Today + Logname):
        os.makedirs(Logs_Folder + Today + Logname)
    SOLR_Log = Logs_Folder + Today + Logname + Settings["SOLR_Log"]
    Errorlog = Logs_Folder + Today + Logname + Settings["Errorlog"]
    Debuglevel = int(Settings["Debuglevel"])
except Exception as E:
    print("Error setting up:")
    print(str(E))
    sys.exit()

UpdateURL = SOLR_Base + "update/json/docs?commitWithin=5000"
DeleteURL = SOLR_Base + "update/json?commitWithin=5000"
headers = {"Content-Type": "application/json"}
if Debuglevel > 0:
    NumOfFiles = len(os.listdir(Delete_Folder))
    with open(SOLR_Log, "a") as file:
        file.write(str(datetime.datetime.now()) + "\n")
        file.write("Starting deletion of records" + "\n")
        file.write("Found " + str(NumOfFiles) + " in " + Delete_Folder + "\n" + "\n")
for del_json in os.listdir(Delete_Folder):
    try:
        if Debuglevel > 1:
            with open(SOLR_Log, "a") as file:
                file.write(str(datetime.datetime.now()) + "\n")
                file.write("Attempting to delete " + str(del_json) + "\n")
        ToDelete = {"delete":{"id":del_json.split(".")[0]}}
        r = requests.post(DeleteURL, headers=headers, json=ToDelete)
        if r.status_code != 200:
            with open(Errorlog, "a") as file:
                file.write(str(datetime.datetime.now()) + "\n")
                file.write("Received error code: " + str(r.status_code) + "\n")
                file.write(str(r.json()) + "\n")
                file.write("The error happened with the json: " + del_json + "\n")
                file.write(str(ToDelete) + "\n")
                file.write("---------------------------------------" + "\n")
        else:
            if Debuglevel > 1:
                with open(SOLR_Log, "a") as file:
                    file.write(str(datetime.datetime.now()) + "\n")
                    file.write("Deletion successfull: " + str(del_json) + "\n")
            os.rename(Delete_Folder + del_json, Del_MyK10 + del_json)
            if Debuglevel > 1:
                with open(SOLR_Log, "a") as file:
                    file.write(str(datetime.datetime.now()) + "\n")
                    file.write("Moved:" + "\n")
                    file.write(Delete_Folder + del_json + "\n")
                    file.write("->" + "\n")
                    file.write(Del_MyK10 + del_json + "\n" + "\n")
    except Exception as E:
        with open(Errorlog, "a") as file:
            file.write(str(datetime.datetime.now()) + "\n")
            file.write(DeleteURL + "\n")
            file.write(str(ToDelete) + "\n")
            file.write(str(E) + "\n")
            file.write(str(traceback.format_exc()) + "\n")
            file.write("---------------------------------------" + "\n")
if Debuglevel > 0:
    NumOfFiles = len(os.listdir(New_Folder))
    with open(SOLR_Log, "a") as file:
        file.write(str(datetime.datetime.now()) + "\n")
        file.write("Finished deleting." + "\n")
        file.write("Starting exporting new .jsons" + "\n")
        file.write("Found " + str(NumOfFiles) + " in " + New_Folder + "\n" + "\n")
for new_json in os.listdir(New_Folder):
    try:
        if Debuglevel > 1:
            with open(SOLR_Log, "a") as file:
                file.write(str(datetime.datetime.now()) + "\n")
                file.write("Attempting to export " + str(new_json) + "\n")
        with open(New_Folder + new_json, "r") as file:
            cur_json = json.load(file)
        r = requests.post(UpdateURL, headers=headers, json=cur_json)
        if r.status_code != 200:
            with open(Errorlog, "a") as file:
                file.write(str(datetime.datetime.now()) + "\n")
                file.write("Received error code: " + str(r.status_code) + "\n")
                file.write(str(r.json()) + "\n")
                file.write("The error happened with the json: " + new_json + "\n")
                file.write(str(cur_json) + "\n")
                file.write("---------------------------------------" + "\n")
        else:
            if Debuglevel > 1:
                with open(SOLR_Log, "a") as file:
                    file.write(str(datetime.datetime.now()) + "\n")
                    file.write("Export successfull: " + str(new_json) + "\n")
            os.rename(New_Folder + new_json, In_MyK10 + new_json)
            if Debuglevel > 1:
                with open(SOLR_Log, "a") as file:
                    file.write(str(datetime.datetime.now()) + "\n")
                    file.write("Moved:" + "\n")
                    file.write(New_Folder + new_json + "\n")
                    file.write("->" + "\n")
                    file.write(In_MyK10 + new_json + "\n" + "\n")
    except Exception as E:
        with open(Errorlog, "a") as file:
            file.write(str(datetime.datetime.now()) + "\n")
            file.write(UpdateURL + "\n")
            file.write(str(cur_json) + "\n")
            file.write(str(E) + "\n")
            file.write(str(traceback.format_exc()) + "\n")
            file.write("---------------------------------------" + "\n")
if Debuglevel > 0:
    with open(SOLR_Log, "a") as file:
        file.write(str(datetime.datetime.now()) + "\n")
        file.write("Finished exporting new .jsons" + "\n" + "\n")
        file.write("Finished Solr_Export.")
