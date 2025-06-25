import datetime
import hashlib
import json
import os
import sqlite3
import sys
import traceback
import time

import requests
from bs4 import BeautifulSoup

from lib.Catalog import check_catalog
from lib.Metadatahandling import gather_metadata, write_delete

if len(sys.argv) > 1:
    Settingsfile = sys.argv[1]
else:
    Settingsfile = "Settings/Settings_Volltext.json"

try:
    with open(Settingsfile, "r") as file:
        Settings = json.load(file)
except Exception as E:
    print("Error loading Settingsfile:")
    print(str(E))
    sys.exit()

try:
    Database = Settings["Database"]
    Debuglevel = int(Settings["Debuglevel"])
    New_Folder = Settings["New_Folder"]
    Delete_Folder = Settings["Delete_Folder"]
    Logs_Folder = Settings["Logs_Folder"]
    Format_File = Settings["Format_File"]
    Relations_File = Settings["Relations_File"]
    ID_Prefix = Settings["ID_Prefix"]
    Marc_Folder = Settings["Marc_Folder"]
    Keep_Marc = bool(int(Settings["Keep_Marc"]))
    InMyK10_Folder = Settings["InMyK10_Folder"]
    Allowlist_PPN_File = Settings["Allowlist_PPN"]
    Denylist_PPN_File = Settings["Denylist_PPN"]

    if Allowlist_PPN_File:
        with open(Allowlist_PPN_File, "r") as file:
            Allowlist_PPN = [line.rstrip() for line in file]
    else:
        Allowlist_PPN = []

    if Denylist_PPN_File:
        with open(Denylist_PPN_File, "r") as file:
            Denylist_PPN = [line.rstrip() for line in file]
    else:
        Denylist_PPN = []

    Lists_PPN = (Allowlist_PPN, Denylist_PPN)

    with open(Format_File, "r") as file:
        Formats = json.load(file)
    with open(Relations_File, "r") as file:
        Relations = json.load(file)
    Today = datetime.datetime.strftime(
        datetime.datetime.today(), '%Y-%m-%d_%H-%M-%S')
    if not os.path.isdir(New_Folder):
        os.makedirs(New_Folder)
    if not os.path.isdir(Delete_Folder):
        os.makedirs(Delete_Folder)
    if not os.path.isdir(Marc_Folder):
        os.makedirs(Marc_Folder)
    Logs_Folder_Today = Logs_Folder + Today
    if not os.path.isdir(Logs_Folder_Today + "_Harvest/"):
        os.makedirs(Logs_Folder_Today + "_Harvest/")
    Logfile = Logs_Folder_Today + "_Harvest/" + Settings["Logfile"]
    Errorlog = Logs_Folder_Today + "_Harvest/" + Settings["Errorlog"]
    with open(Errorlog, "a") as file:
        error_appeared = False
except Exception as E:
    print("Error setting up:")
    print(str(E))
    sys.exit()

headers = {"User-Agent": "ulbbot+myk10harvest"}


Total_Time = datetime.datetime.now()
Total_Total_Length = 0

for DataSource in Settings["DataSources"]:
    Default_Data = (DataSource, Formats, Relations)
    metadatatype = DataSource["MetadataType"]
    BaseUrl = DataSource["BaseURL"]
    Refresh_Days = int(DataSource["Refresh_Days"])
    Full_Refresh = int(DataSource["Full_Refresh"])
    Collection_Handles = DataSource["Collection_Handles"]
    Catalog_Check = int(DataSource["Catalog_Check"])

    if not os.path.isfile(Database):
        if "/" in Database:
            DBF = Database.rsplit("/", 1)[0]
            if not os.path.exists(DBF):
                os.makedirs(DBF)
        con = sqlite3.connect(Database)
        cur = con.cursor()
        cur.execute("CREATE TABLE Metadata(Meta, Data)")
        cur.execute(
            "CREATE TABLE Records(Identifier, Metahash, UUID PRIMARY KEY, Status, MyK10)")
        cur.execute(
            "CREATE TABLE Url_Settings(BaseURL PRIMARY KEY, LastModified, LastFull)")

        cur.execute("CREATE INDEX Identities ON Records(Identifier)")
        cur.execute("CREATE INDEX Metahashes ON Records(Metahash)")
        cur.execute(
            "CREATE INDEX IdentityHash ON Records(Identifier, Metahash)")

        cur.execute("INSERT INTO Metadata VALUES(?, ?)",
                    ("Settings", Settingsfile))
        cur.execute("INSERT INTO Metadata VALUES(?, ?)", ("Highest_ID", 0))
        cur.execute("INSERT INTO Metadata VALUES(?, ?)",
                    ("LastRun_Duration", 0))
        cur.execute("INSERT INTO Metadata VALUES(?, ?)",
                    ("LastRun_TotalRecords", 0))
        MAX_LastModified = datetime.datetime.min
        cur.execute("INSERT INTO Url_Settings VALUES(?, ?, ?)", (BaseUrl, MAX_LastModified.strftime(
            "%Y-%m-%d"), MAX_LastModified.strftime("%Y-%m-%d")))
        ResumptionToken = metadatatype + "////0"
        FullRun = True
        Current_ID = 0
        con.commit()
        con.close()

    else:
        con = sqlite3.connect(Database)
        cur = con.cursor()
        Url_Settings_DB = list(cur.execute(
            "SELECT * FROM Url_Settings WHERE BaseURL = '" + BaseUrl + "'"))

        Current_ID_DB = list(cur.execute(
            "SELECT * FROM Metadata WHERE Meta = 'Highest_ID'"))
        Current_ID = int(Current_ID_DB[0][-1])
        if Url_Settings_DB:
            (BaseURL, MAX_LastModified_DB,
             Last_Full_Run_DB) = Url_Settings_DB[0]
            Last_Full_Run = Last_Full_Run_DB
            # Only needed if script did not finish first run
            for i in range(4-len(Last_Full_Run_DB.split("-")[0])):
                Last_Full_Run = "0" + Last_Full_Run
            Last_Full_Run = datetime.datetime.strptime(
                Last_Full_Run, '%Y-%m-%d')
            FullRun = False
            # Since the script doesn't start at the exact same time, we need to go to seconds and scale it up (days will get rounded)
            if (Total_Time - Last_Full_Run).total_seconds() > Full_Refresh * 86400 - 3600:
                MAX_LastModified = datetime.datetime.min  # Needs a full check
                FullRun = True
            else:
                MAX_LastModified = MAX_LastModified_DB
                # Only needed if script did not finish first run
                for i in range(4-len(MAX_LastModified_DB.split("-")[0])):
                    MAX_LastModified = "0" + MAX_LastModified
                MAX_LastModified = datetime.datetime.strptime(
                    MAX_LastModified, '%Y-%m-%d')
                if MAX_LastModified > datetime.datetime.min:
                    MAX_LastModified = MAX_LastModified - \
                        datetime.timedelta(days=Refresh_Days)
                else:
                    MAX_LastModified = datetime.datetime.min
                    FullRun = True
        else:
            MAX_LastModified = datetime.datetime.min
            FullRun = True
            cur.execute("INSERT INTO Url_Settings VALUES(?, ?, ?)", (BaseUrl, MAX_LastModified.strftime(
                "%Y-%m-%d"), MAX_LastModified.strftime("%Y-%m-%d")))
            con.commit()
        MAX_LastModified_Str = MAX_LastModified.strftime("%Y-%m-%d")
        ResumptionToken = metadatatype + "/" + MAX_LastModified_Str + "///0"
        con.close()

    con = sqlite3.connect(Database)
    cur = con.cursor()
    
    if Debuglevel > 0:
        with open(Logfile, "a") as file:
            file.write(str(datetime.datetime.now()) + "\n")
            file.write("BaseURL:" + "\n")
            file.write(str(BaseUrl) + "\n")
            file.write("Metadatatype:" + "\n")
            file.write(metadatatype + "\n" + "\n")
            if FullRun:
                file.write("\n" + "Starting a full run." + "\n" + "\n")

    Tokens = []
    if Collection_Handles:
        ResTokSplit = ResumptionToken.split("/")
        for Col in Collection_Handles:
            SplitCol = Col.split("/")
            col = "col_" + SplitCol[0] + "_" + SplitCol[-1]
            Tokens.append(ResTokSplit[0] + "/" + ResTokSplit[1] +
                          "/" + ResTokSplit[2] + "/" + col + "/" + ResTokSplit[4])
    else:
        Tokens.append(ResumptionToken)
    Total_Length = 0
    for ResumptionToken in Tokens:
        Total_Count = 0
        Counter = 0
        while ResumptionToken:
            TimePerPage = datetime.datetime.now()
            Request = BaseUrl + ResumptionToken
            ResumptionTokenOld = ResumptionToken
            try:
                if Debuglevel > 1:
                    with open(Logfile, "a") as file:
                        file.write(str(datetime.datetime.now()) + "\n")
                        file.write("Now trying to load:" + "\n")
                        file.write(Request + "\n" + "\n")
                r = requests.get(Request, headers=headers)
                soup = BeautifulSoup(r.text, features="xml")
                ResumptionToken = soup.find("resumptionToken").text
                Length = soup.find("resumptionToken").get("completeListSize")
                records = soup.find_all("record")
            except Exception as E:
                with open(Errorlog, "a") as file:
                    error_appeared = True
                    file.write(str(datetime.datetime.now()) + "\n")
                    file.write(Request + "\n")
                    file.write(str(E) + "\n")
                    file.write(str(traceback.format_exc()) + "\n" + "\n")
                records = []
                ResumptionToken = ResumptionTokenOld
                time.sleep(60)
            for record in records:
                try:
                    CUR_ID = record.find("identifier").text
                    if Debuglevel > 2:
                        with open(Logfile, "a") as file:
                            file.write(str(datetime.datetime.now()) + "\n")
                            file.write("Found record: " + CUR_ID + "\n")
                    header = record.find("header")
                    if header.get("status") == "deleted":
                        CUR_Status = "deleted"
                        metadata = None
                        CUR_Meta = hashlib.sha256(b"").hexdigest()
                    else:
                        CUR_Status = "available"
                        metadata = record.find("metadata")
                        CUR_Meta = hashlib.sha256(
                            str(metadata).encode()).hexdigest()

                    CUR_LastModified = datetime.datetime.strptime(
                        header.find("datestamp").text, "%Y-%m-%dT%H:%M:%SZ")
                    if CUR_LastModified > MAX_LastModified:
                        MAX_LastModified_Str = CUR_LastModified.strftime(
                            "%Y-%m-%d")
                        MAX_LastModified = CUR_LastModified
                        if Debuglevel > 2:
                            with open(Logfile, "a") as file:
                                file.write(str(datetime.datetime.now()) + "\n")
                                file.write("Found new LastModified: " +
                                           MAX_LastModified_Str + "\n")

                    # Check if ID was already seen
                    RecordDataFromDB = list(cur.execute(
                        "SELECT * FROM Records WHERE Identifier = '" + CUR_ID + "' AND Metahash = '" + CUR_Meta + "'"))
                    if not RecordDataFromDB:
                        if metadata:
                            RecordDataFromDB = list(cur.execute(
                                "SELECT * FROM Records WHERE Identifier = '" + CUR_ID + "' OR Metahash = '" + CUR_Meta + "'"))
                        else:
                            RecordDataFromDB = list(cur.execute(
                                "SELECT * FROM Records WHERE Identifier = '" + CUR_ID + "'"))
                    if len(RecordDataFromDB) > 1:
                        with open(Errorlog, "a") as file:
                            error_appeared = True
                            file.write(str(datetime.datetime.now()) + "\n")
                            file.write(
                                "More than one record found with SELECT" + "\n")
                            for RDFDB in RecordDataFromDB:
                                file.write(str(RDFDB) + "\n")
                            file.write("Found more than one DB entry!" + "\n")
                            file.write(str(traceback.format_exc()) + "\n")
                            file.write("Quitting" + "\n")
                        sys.exit()

                    if RecordDataFromDB:
                        if Debuglevel > 2:
                            with open(Logfile, "a") as file:
                                file.write(str(datetime.datetime.now()) + "\n")
                                file.write("Found record in DB" + "\n")
                                file.write(str(RecordDataFromDB[0]) + "\n")
                        (DB_ID, DB_Meta, DB_UUID, DB_Status,
                         DB_MyK10) = RecordDataFromDB[0]
                        Old_Data = (metadata, header, DB_ID, DB_Meta, DB_UUID)
                        New_Data = (metadata, header, CUR_ID,
                                    CUR_Meta, DB_UUID)

                        if DB_ID != CUR_ID or DB_Meta != CUR_Meta:  # There was a change
                            if CUR_Status != "deleted":  # There was a change, and CUR_Status = available
                                try:
                                    in_catalog = check_catalog(
                                        metadata, metadatatype, Catalog_Check, Lists_PPN)
                                    if Debuglevel > 2:
                                        with open(Logfile, "a") as file:
                                            file.write(
                                                str(datetime.datetime.now()) + "\n")
                                            file.write(
                                                "in_catalog result:" + "\n")
                                            file.write(str(in_catalog) + "\n")
                                except Exception as E:
                                    with open(Errorlog, "a") as file:
                                        error_appeared = True
                                        file.write(
                                            str(datetime.datetime.now()) + "\n")
                                        file.write(
                                            "Error while doing catalogue lookup:" + "\n")
                                        file.write(str(E) + "\n")
                                        file.write(
                                            str(traceback.format_exc()) + "\n" + "\n")
                                    in_catalog = False
                                if not in_catalog:
                                    if DB_MyK10:  # There was a change, CUR_Status = available, and not in catalog, and in MyK10+
                                        write_delete(
                                            Old_Data, InMyK10_Folder, Delete_Folder, Debuglevel, Logfile)
                                        gather_metadata(
                                            New_Data, Default_Data, metadatatype, Marc_Folder, New_Folder, Logs_Folder, Debuglevel, Logfile, Errorlog)
                                    else:  # There was a change, CUR_Status = available, and not in catalog, and not in MyK10+
                                        gather_metadata(
                                            New_Data, Default_Data, metadatatype, Marc_Folder, New_Folder, Logs_Folder, Debuglevel, Logfile, Errorlog)
                                        DB_MyK10 = 1
                                else:
                                    if DB_MyK10:  # There was a change, CUR_Status = available, and in MyK10+, and in Catalog
                                        write_delete(
                                            Old_Data, InMyK10_Folder, Delete_Folder, Debuglevel, Logfile)
                                        DB_MyK10 = 0
                                    else:  # There was a change, CUR_Status = available, and not in MyK10+, and in Catalog
                                        pass  # Nothing to do
                            else:  # There was a change, and CUR_Status = deleted
                                if DB_Status != "deleted":  # Not deleted before
                                    if DB_MyK10:  # In MyK10+, but locally deleted
                                        write_delete(
                                            Old_Data, InMyK10_Folder, Delete_Folder, Debuglevel, Logfile)
                                        DB_MyK10 = 0
                                    else:  # There was change, it is deleted, it wasn't deleted, it isn't in MyK10+
                                        pass
                                else:  # Was deleted before, does not matter
                                    # There shouldn't even be a change in this case, not sure if this ever gets executed
                                    pass
                            Exestr = "UPDATE Records SET Identifier = '" + CUR_ID + "', Metahash = '" + CUR_Meta + "', Status = '" + \
                                CUR_Status + "', MyK10 = " + \
                                str(DB_MyK10) + " WHERE Identifier = '" + \
                                DB_ID + "' AND Metahash = '" + DB_Meta + "'"
                            cur.execute(Exestr)
                            if Debuglevel > 2:
                                with open(Logfile, "a") as file:
                                    file.write(
                                        str(datetime.datetime.now()) + "\n")
                                    file.write(Exestr + "\n" + "\n")
                        else:  # No changes
                            if DB_MyK10:
                                try:
                                    in_catalog = check_catalog(
                                        metadata, metadatatype, Catalog_Check, Lists_PPN)
                                    if Debuglevel > 2:
                                        with open(Logfile, "a") as file:
                                            file.write(
                                                str(datetime.datetime.now()) + "\n")
                                            file.write(
                                                "in_catalog result:" + "\n")
                                            file.write(str(in_catalog) + "\n")
                                except Exception as E:
                                    with open(Errorlog, "a") as file:
                                        error_appeared = True
                                        file.write(
                                            str(datetime.datetime.now()) + "\n")
                                        file.write(
                                            "Error while doing catalogue lookup:" + "\n")
                                        file.write(str(E) + "\n")
                                        file.write(
                                            str(traceback.format_exc()) + "\n")
                                    in_catalog = False
                                if in_catalog:  # No local change, but record is available in catalog -> Delete from MyK10+
                                    write_delete(
                                        Old_Data, InMyK10_Folder, Delete_Folder, Debuglevel, Logfile)
                                    DB_MyK10 = 0
                                    Exestr = "UPDATE Records SET Identifier = '" + CUR_ID + "', Metahash = '" + CUR_Meta + "', Status = '" + \
                                        CUR_Status + "', MyK10 = " + \
                                        str(DB_MyK10) + " WHERE Identifier = '" + \
                                        DB_ID + "' AND Metahash = '" + DB_Meta + "'"
                                    cur.execute(Exestr)
                                    if Debuglevel > 2:
                                        with open(Logfile, "a") as file:
                                            file.write(
                                                str(datetime.datetime.now()) + "\n")
                                            file.write(Exestr + "\n" + "\n")
                                else:  # No changes, in MyK10+, and not in catalog -> Working as intended
                                    if Debuglevel > 2:
                                        with open(Logfile, "a") as file:
                                            file.write(
                                                str(datetime.datetime.now()) + "\n")
                                            file.write(
                                                "No changes" + "\n" + "\n")
                            else:
                                # Assuming things don't get deleted from the catalog
                                if Debuglevel > 2:
                                    with open(Logfile, "a") as file:
                                        file.write(
                                            str(datetime.datetime.now()) + "\n")
                                        file.write("No changes" + "\n" + "\n")
                    else:  # New record
                        if Debuglevel > 2:
                            with open(Logfile, "a") as file:
                                file.write(str(datetime.datetime.now()) + "\n")
                                file.write("Record not in DB" + "\n")
                        # Generate MyK10+ ID
                        # ID_Prefix + 9 symbols [A-Z0-9]
                        # hex(68719476735) = FFFFFFFFF -> No more room without adding a digit
                        if Current_ID >= 68719476735:
                            with open(Errorlog, "a") as file:
                                error_appeared = True
                                file.write(str(datetime.datetime.now()) + "\n")
                                file.write(
                                    "Maximum number of possible IDs reached.\n")
                                file.write(
                                    "Please use another way to create IDs, or change prefix.\n")
                                file.write("The script will now stop" + "\n\n")
                                sys.exit()
                        else:
                            # Update Current_ID in database first, so no duplicate IDs will exist
                            cur.execute("UPDATE Metadata SET Data = '" +
                                        str(Current_ID + 1) + "' WHERE Meta = 'Highest_ID'")
                            con.commit()
                            CUR_UUID = hex(Current_ID)[2:].upper()
                            Current_ID = Current_ID + 1
                            while len(CUR_UUID) < 9:
                                CUR_UUID = "0" + CUR_UUID
                            CUR_UUID = ID_Prefix + CUR_UUID
                        if CUR_Status != "deleted":
                            try:
                                in_catalog = check_catalog(
                                    metadata, metadatatype, Catalog_Check, Lists_PPN)
                                if Debuglevel > 2:
                                    with open(Logfile, "a") as file:
                                        file.write(
                                            str(datetime.datetime.now()) + "\n")
                                        file.write("in_catalog result:" + "\n")
                                        file.write(str(in_catalog) + "\n")
                            except Exception as E:
                                with open(Errorlog, "a") as file:
                                    error_appeared = True
                                    file.write(
                                        str(datetime.datetime.now()) + "\n")
                                    file.write(
                                        "Error doing catalogue lookup" + "\n")
                                    file.write(str(E) + "\n")
                                    file.write(
                                        str(traceback.format_exc()) + "\n" + "\n")
                                in_catalog = False
                            if not in_catalog:  # Available and not in catalog -> MyK10+
                                gather_metadata((metadata, header, CUR_ID, CUR_Meta, CUR_UUID), Default_Data,
                                                metadatatype, Marc_Folder, New_Folder, Logs_Folder, Debuglevel, Logfile, Errorlog)
                                DB_MyK10 = 1
                            else:  # Available and in catalog -> Not MyK10+
                                DB_MyK10 = 0
                        else:  # Deleted and new -> Not MyK10+
                            DB_MyK10 = 0
                        cur.execute("INSERT INTO Records VALUES(?, ?, ?, ?, ?)",
                                    (CUR_ID, CUR_Meta, CUR_UUID, CUR_Status, DB_MyK10))
                        if Debuglevel > 2:
                            with open(Logfile, "a") as file:
                                file.write(str(datetime.datetime.now()) + "\n")
                                file.write("Inserted in local DB:" + "\n")
                                file.write(
                                    str((CUR_ID, CUR_Meta, CUR_UUID, CUR_Status, DB_MyK10)) + "\n" + "\n")
                    con.commit()
                    Counter += 1
                except Exception as E:
                    with open(Errorlog, "a") as file:
                        error_appeared = True
                        file.write(str(datetime.datetime.now()) + "\n")
                        file.write(str(Request) + "\n")
                        file.write(str(E) + "\n")
                        file.write(str(traceback.format_exc()) + "\n" + "\n")
                    ResumptionToken = ResumptionTokenOld
            if Debuglevel > 1:
                with open(Logfile, "a") as file:
                    file.write(str(datetime.datetime.now()) + "\n")
                    file.write(str(Counter) + "/" + str(Length) + " - " + str(
                        (datetime.datetime.now() - TimePerPage).total_seconds()) + "\n" + "\n")

        if not FullRun:  # No need to update LastFull
            cur.execute("UPDATE Url_Settings SET LastModified = '" +
                        MAX_LastModified_Str + "' WHERE BaseURL = '" + BaseUrl + "'")
            if Debuglevel > 1:
                with open(Logfile, "a") as file:
                    file.write(str(datetime.datetime.now()) + "\n")
                    file.write("UPDATE Url_Settings SET LastModified = '" +
                               MAX_LastModified_Str + "' WHERE BaseURL = '" + BaseUrl + "'" + "\n" + "\n")
        else:
            cur.execute("UPDATE Url_Settings SET LastModified = '" + MAX_LastModified_Str +
                        "', LastFull = '" + Total_Time.strftime("%Y-%m-%d") + "' WHERE BaseURL = '" + BaseUrl + "'")
            if Debuglevel > 1:
                with open(Logfile, "a") as file:
                    file.write(str(datetime.datetime.now()) + "\n")
                    file.write("UPDATE Url_Settings SET LastModified = '" + MAX_LastModified_Str +
                               "', LastFull = '" + Total_Time.strftime("%Y-%m-%d") + "' WHERE BaseURL = '" + BaseUrl + "'\n\n")
        Total_Count = Total_Count + Counter
        Total_Length = Total_Length + int(Length)
        Total_Total_Length = Total_Total_Length + Total_Length
        con.commit()

Total_Time = (datetime.datetime.now() - Total_Time).total_seconds()

cur.execute("UPDATE MetaData SET Data = '" + str(Total_Time) +
            "' WHERE Meta = 'LastRun_Duration'")
cur.execute("UPDATE MetaData SET Data = '" + str(Total_Total_Length) +
            "' WHERE Meta = 'LastRun_TotalRecords'")
con.commit()
con.close()
if Debuglevel > 0:
    with open(Logfile, "a") as file:
        file.write(str(datetime.datetime.now()) + "\n")
        file.write("Finished " + str(Total_Total_Length) +
                   " records in " + str(Total_Time) + " seconds." + "\n")
# Cleanup step
if not Keep_Marc:
    if os.path.isdir(Marc_Folder):
        for file in os.listdir(Marc_Folder):
            os.remove(Marc_Folder + file)
        os.rmdir(Marc_Folder)
if not error_appeared:
    os.remove(Errorlog)
if len(os.listdir(Logs_Folder_Today + "_Harvest/")) < 1:
    os.rmdir(Logs_Folder_Today + "_Harvest/")
if len(os.listdir(Logs_Folder)) < 1:
    os.rmdir(Logs_Folder)
