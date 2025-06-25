import datetime
import pymarc
import json
import requests
import shutil
import os.path
import sys


# Create a file given a DataDict
def write_new_file(DataDict, New_Folder, Debuglevel, Logfile):
    with open(New_Folder + str(DataDict["id"]) + ".json", "w") as file:
        json.dump(DataDict, file, allow_nan=True, indent=4)
    if Debuglevel > 2:
        with open(Logfile, "a") as file:
            file.write(str(datetime.datetime.now()) + "\n")
            file.write("Wrote new file:" + "\n")
            file.write(str(DataDict["id"]) + "\n")


# Record has to be deleted from MyK10+
def write_delete(Available_Data, InMyK10_Folder, Delete_Folder, Debuglevel, Logfile):
    (metadata, header, id, metahash, uuid) = Available_Data
    Targetfile = uuid + ".json"
    if os.path.exists(InMyK10_Folder + Targetfile):
        shutil.move(InMyK10_Folder + Targetfile, Delete_Folder +
                    Targetfile)  # Mark for deletion via Solr_Export
        if Debuglevel > 2:
            with open(Logfile, "a") as file:
                file.write(str(datetime.datetime.now()) + "\n")
                file.write("Moved file to delete:" + "\n")
                file.write(str(uuid) + "\n")
    else:
        if Debuglevel > 0:
            with open(Logfile, "a") as file:
                file.write(str(datetime.datetime.now()) + "\n")
                file.write(
                    "Tried to move file to delete, but file was not found:" + "\n")
                file.write(str(uuid) + "\n")


# Creates the MARC-Metadata for the record
def get_marc(Available_Data, DataDict, Additional_Data, DataSource, Marc_Folder):
    def make_xml(ListOfFields, ListOfControlFields):
        MarcXML = pymarc.Record()
        for (tag, data) in ListOfControlFields:
            MarcXML.add_field(pymarc.Field(tag=tag, data=data))
        for (tag, ind1, ind2, ListOfSubfields) in ListOfFields:
            Subfields = []
            for (code, content) in ListOfSubfields:
                Subfields.append(pymarc.Subfield(code=code, value=content))
            Field = pymarc.Field(tag=tag, indicators=[
                                 ind1, ind2], subfields=Subfields)
            MarcXML.add_field(Field)
        # Leader calculation
        leader = MarcXML.as_marc().decode("UTF-8")[0:24]
        MarcXML.leader = leader[0:5] + "n"
        if DataDict["format_phy_str_mv"] == "Book" or DataDict["format_phy_str_mv"] == "Article":
            typeofrecord = "am"
        else:
            if DataDict["format_phy_str_mv"] == "Mixed Materials":
                typeofrecord = "pm"
            else:
                if DataDict["format_phy_str_mv"] == "Dataset":
                    typeofrecord = "mm"
                else:
                    if DataDict["format_phy_str_mv"] == "Monograph Series":
                        typeofrecord = "ac"
                    else:
                        if DataDict["format_phy_str_mv"] == "Journal":
                            typeofrecord = "as"
                        else:
                            typeofrecord = "am"

        MarcXML.leader = MarcXML.leader + typeofrecord
        MarcXML.leader = MarcXML.leader + leader[8:18] + "a" + leader[19:]
        writer = pymarc.XMLWriter(
            open(Marc_Folder + str(Available_Data[4]) + ".xml", "wb"))
        writer.write(MarcXML)
        writer.close()
        with open(Marc_Folder + str(Available_Data[4]) + ".xml", "r") as file:
            MarcXML_String = file.read()
        return MarcXML_String

    (metadata, header, id, metahash, uuid) = Available_Data
    ListOfFields = [("264", "#", "2", [
                     ("b", DataSource["Publisher"]), ("c", Additional_Data["AvailableDate"])])]

    # Control fields
    ListOfControlFields = [("001", uuid)]  # Control Number
    ListOfControlFields.append(("003", "DE-3"))  # Control Number Identifier
    # Date and time of latest transaction
    ListOfControlFields.append(("005", datetime.datetime.strptime(
        DataDict["up_date"], "%Y-%m-%dT%H:%M:%SZ").strftime("%Y%m%d%H%M%S.0")))
    # 008
    # Date entered on file
    ZeroZeroEight = datetime.datetime.strptime(
        DataDict["up_date"], "%Y-%m-%dT%H:%M:%SZ").strftime("%y%m%d")
    # Type of date/Publication status
    try:
        ZeroZeroEight = ZeroZeroEight + "s" + \
            DataDict["publishDateSort"][0:4] + "####"
    except:
        ZeroZeroEight = ZeroZeroEight + "n" + "||||" + "||||"
    # Place of publication, production, or execution
    ZeroZeroEight = ZeroZeroEight + "xx#"
    # Material specific coded elements (starts with 18, ends with 34)
    ZeroZeroEight = ZeroZeroEight + "#"\
        + "#"\
        + "#"\
        + "#"\
        + "#"\
        + "o"\
        + "#"\
        + "#"\
        + "|"\
        + "#"\
        + "#"\
        + "######"
    # Language
    if "lang_code" in DataDict.keys():
        ZeroZeroEight = ZeroZeroEight + DataDict["lang_code"]
    else:
        ZeroZeroEight = ZeroZeroEight + "|||"
    # Modified record
    ZeroZeroEight = ZeroZeroEight + "#"
    # Cataloging source
    ZeroZeroEight = ZeroZeroEight + "#"

    ListOfControlFields.append(("008", ZeroZeroEight))

    # DataDict
    # Collections
    if "collection_details" in DataDict.keys():
        for col in DataDict["collection_details"]:
            ListOfFields.append(("912", "#", "#", [("a", col)]))

    # title
    if "title" in DataDict.keys():
        ListOfFields.append(("245", "0", "0", [("a", DataDict["title"])]))
    # author
    if "author" in DataDict.keys():
        ListOfSubfields = []
        author = DataDict["author"]
        if author in Additional_Data["authorities"].keys():
            for author_id in Additional_Data["authorities"][author]:
                ListOfSubfields.append(("0", author_id))
        ListOfSubfields.append(("a", author))
        ListOfSubfields.append(("e", "verfasserin"))
        ListOfFields.append(("100", "1", "#", ListOfSubfields))
    # other persons
    if "other_persons" in Additional_Data.keys():
        for (name, role, roleshort) in Additional_Data["other_persons"]:
            ListOfSubfields = []
            if name in Additional_Data["authorities"].keys():
                for person_id in Additional_Data["authorities"][name]:
                    ListOfSubfields.append(("0", person_id))
            ListOfSubfields.append(("a", name))
            ListOfSubfields.append(("e", role))
            ListOfSubfields.append(("4", roleshort))
            ListOfFields.append(("700", "#", "#", ListOfSubfields))
    # subject
    if "topic_facet" in DataDict.keys():
        for subject in DataDict["topic_facet"]:
            ListOfSubfields = [("a", subject)]
            ListOfFields.append(("689", "#", "4", ListOfSubfields))
    # language
    if "lang_code" in DataDict.keys():
        ListOfFields.append(("041", "#", "#", [("a", DataDict["lang_code"])]))
    # Fulltext urls
    if "foreign_ids_str_mv" in DataDict.keys():
        Skip = False
        for (identifier, source) in DataDict["foreign_ids_str_mv"]:
            if source == "doi":
                ListOfSubfields = [("r", identifier)]
            else:
                if source == "URN":
                    ListOfSubfields = [("r", "https://nbn-resolving.de/" + identifier)]
                else:
                    # Source neither URN nor DOI, don't know what to do
                    ListOfSubfields = []
            if ListOfSubfields and not Skip:
                if "openaccess" in Additional_Data.keys():
                    if "false" == Additional_Data["openaccess"]:
                        ListOfSubfields.append(("y", "Der Zugang zur Ressource ist auf einen bestimmten Personenkreis beschränkt. Bei Fragen wenden Sie sich bitte an die Betreiber des Repositoriums."))
                    else:
                        ListOfSubfields.append(("y", "Kostenfrei in unserem Repositorium aufrufbar"))
                ListOfSubfields.append(("1", "01"))
                ListOfSubfields.append(("2", "65"))
                ListOfFields.append(("981", "#", "#", ListOfSubfields))
                Skip = True # Only 1 URL
    if "mirador" in Additional_Data.keys():
        ListOfFields.append(("981", "#", "#", [("1", "01"), ("2", "65"), ("r", Additional_Data["mirador"]), ("y", "Mirador")]))
    # Identifier
    if "foreign_ids_str_mv" in DataDict.keys():
        for (identifier, source) in DataDict["foreign_ids_str_mv"]:
            ListOfSubfields = []
            identype = False
            idenurl = False
            i1 = "#"
            i2 = "#"
            if source == "doi":
                identype = source
                idenurl = identifier.split("org/")[-1]
                i1 = "7"
                i2 = "0"
            else:
                if source == "URN":
                    identype = source
                    idenurl = identifier
                    i1 = "7"
                    i2 = "0"
                else:
                    if source == "PPN":
                        idenurl = "(DE-627)" + identifier  # K10+
                    else:
                        if "vd" in source.lower():
                            idenurl = identifier.upper()
                            identype = source
                            i1 = "7"
                        else:
                            if source == "fingerprint":
                                idenurl = identifier
                                ListOfSubfields.append(("5", "DE-3"))
                                identype = "fei"
            if ListOfSubfields:  # Fingerprint
                ListOfSubfields.append(("2", identype))
                ListOfSubfields.append(("e", idenurl))
                ListOfFields.append(("026", i1, i2, ListOfSubfields))
            else:
                if not identype:  # PPN
                    ListOfSubfields.append(("035", i2, i2, [("a", idenurl)]))
                else:
                    if idenurl:  # Everything else
                        ListOfSubfields.append(("2", identype))
                        ListOfSubfields.append(("a", idenurl))
                        ListOfFields.append(("024", i1, i2, ListOfSubfields))

    # abstract
    if "description" in Additional_Data.keys():
        for desc in Additional_Data["description"]:
            ListOfFields.append(("520", "3", " ", [("a", desc)]))
    # License
    if "License" in Additional_Data.keys():
        if "creativecommons.org" in Additional_Data["License"]:
            ListOfSubfields = [("2", "cc")]
            ListOfSubfields.append(("a", Additional_Data["License"]))
            ListOfSubfields.append(("q", "DE-3"))
            ListOfSubfields.append(("u", Additional_Data["License"]))
            ListOfFields.append(("540", "#", "#", ListOfSubfields))
        else:
            ListOfFields.append(("540", "#", "#", [
                                ("a", Additional_Data["License"]), ("u", Additional_Data["License"])]))
    # issn
    if "issn" in DataDict.keys():
        ListOfSubfields = [("a", DataDict["issn"])]
        ListOfSubfields.append(
            ("0", "https://portal.issn.org/resource/ISSN/" + DataDict["issn"] + "#ISSN"))
        ListOfFields.append(("022", "#", "#", ListOfSubfields))
    # Journal
    if "journal" in DataDict.keys():
        ListOfSubfields = [("t", DataDict["journal"])]
        if "journal_volume" in Additional_Data.keys():
            ListOfSubfields.append(
                ("g", "Ausgabe " + Additional_Data["journal_volume"]))
            if "journal_pagestart" in Additional_Data.keys():
                ListOfSubfields.append(
                    ("q", Additional_Data["journal_volume"] + "<" + Additional_Data["journal_pagestart"]))
        if "issn" in DataDict.keys():
            ListOfSubfields.append(("x", DataDict["issn"]))
        ListOfFields.append(("773", "1", "#", ListOfSubfields))
    # Relations
    if "relations" in Additional_Data.keys():
        ListOfSubfields = []
        for relation in Additional_Data["relations"]:
            ListOfSubfields.append(("o", relation))
        ListOfFields.append(("787", "1", "#", ListOfSubfields))
        # Currently not showing up in Lukida, may change to
        # 856, 4, 2
    # Series
    if "series" in DataDict.keys():
        ListOfSubfields = [("a", DataDict["series"])]
        if "journal_volume" in Additional_Data.keys():
            ListOfSubfields.append(("v", Additional_Data["journal_volume"]))
        if "issn" in DataDict.keys():
            ListOfSubfields.append(("x", DataDict["issn"]))
        ListOfFields.append(("490", "0", "#", ListOfSubfields))
    # Publisher
    if "publisher_add" in Additional_Data.keys():
        for publisher in Additional_Data["publisher_add"]:
            ListOfSubfields = [("b", publisher)]
            if "publishDateSort" in DataDict.keys():
                ListOfSubfields.append(("c", DataDict["publishDateSort"]))
            ListOfFields.append(("264", "#", "1", ListOfSubfields))
    # Notes
    if "notes" in Additional_Data.keys():
        for note in Additional_Data["notes"]:
            if note[-1] != ".":
                note = note + "."  # Input convention: 500 ends with "."
            ListOfSubfields = [("a", note)]  # a is NR
            ListOfFields.append(("500", "#", "#", ListOfSubfields))


    # DDC
    if "dewey-full" in DataDict.keys():
        ListOfSubfields = []
        for ddc in DataDict["dewey-full"]:
            ListOfFields.append(("082", "7", "#", [("a", ddc)]))
    # Extent
    if "physical" in DataDict.keys():
        ListOfFields.append(("300", "#", "#", [("a", DataDict["physical"])]))

    # Edition
    if "edition" in DataDict.keys():
        ListOfFields.append(("250", "#", "#", [("a", DataDict["edition"])]))

    # ISBN
    if "isbn" in DataDict.keys():
        ListOfFields.append(("020", "#", "#", [("a", DataDict["isbn"])]))
    # Signature
    if "signature_iln" in DataDict.keys():
        ListOfSubfields = []
        for signatur in DataDict["signature_iln"]:
            ListOfSubfields.append(("c", signatur))
        ListOfFields.append(("852", "#", "#", ListOfSubfields))
    # Conference
    if "conference" in Additional_Data.keys():
        ListOfSubfields = [("a", Additional_Data["conference"])]
        if "conference_place" in Additional_Data.keys():
            ListOfSubfields.append(("c", Additional_Data["conference_place"]))
        ListOfFields.append(("111", "2", "#", ListOfSubfields))

    # Genre
    if "genre_facet" in DataDict.keys():
        for genre in DataDict["genre_facet"]:
            ListOfFields.append(("655", "#", "4", [("a", genre)]))
    if "Dataset" == DataDict["format_phy_str_mv"]:
        # Better metadata for research data
        ListOfFields.append(("655", "#", "7", [("0", "(DE-627)857755366"), ("2", "gnd-content"), ("a", "Forschungsdaten")]))
    if "Journal" == DataDict["format_phy_str_mv"]:
        ListOfFields.append(("655", "#", "7", [("0", "(DE-627)106108832"), ("2", "gnd-content"), ("a", "Zeitung")]))
    # Mets
    if "mets" in Additional_Data.keys():
        ListOfFields.append(
            ("590", "#", "#", [("x", "mets"), ("u", Additional_Data["mets"])]))
    # Other entry
    if "SeeOtherEntry" in Additional_Data.keys():
        ListOfFields.append(("776", "0", "#", [("i", "Elektronische Reproduktion von"), ("t", DataDict["title"]), ("w", "(DE-627)" + Additional_Data["SeeOtherEntry"])]))
        # 787 would be better, but does not work natively in lukdia
    if "uniform_title" in Additional_Data.keys():
        uniform_title = Additional_Data["uniform_title"]
        ListOfSubfields = []
        if uniform_title in Additional_Data["authorities"].keys():
            for authority in Additional_Data["authorities"][uniform_title]:
                ListOfSubfields.append(("0", authority))
        ListOfSubfields.append(("a", uniform_title))
        ListOfFields.append(("240", "1", "#", ListOfSubfields))

    return make_xml(sorted(ListOfFields), ListOfControlFields)


def gather_metadata(Available_Data, Default_Data, metadatatype, Marc_Folder, New_Folder, Logs_Folder, Debuglevel, Logfile, Errorlog):  # A new record for MyK10+

    def allfields_calc(allfields, dictentry):
        if isinstance(dictentry, list):
            for item in dictentry:
                if isinstance(item, tuple):
                    for subitem in item:
                        allfields = allfields + " " + subitem
                else:
                    allfields = allfields + " " + item
        else:
            if isinstance(dictentry, dict):
                for secondkey in dictentry.keys():
                    if isinstance(dictentry[secondkey], list):
                        for item in dictentry[secondkey]:
                            allfields = allfields + " " + item
                    else:
                        # Not a list, should be a string
                        allfields = allfields + " " + dictentry[secondkey]
            else:
                allfields = allfields + " " + dictentry
        return allfields

    def write_error_format(typ, meta, handle):
        with open(Logs_Folder + "Formats.txt", "a") as file:
            file.write(str(typ) + " (" + meta + ") - " + handle + "\n")

    def write_error_relations(role, meta, handle):
        with open(Logs_Folder + "Relations.txt", "a") as file:
            file.write(str(role) + " (" + meta + ") - " + handle + "\n")

    def find_authority(field):
        name = field.text
        for sibling in field.find_next_siblings(attrs={"name": "authority"}):
            found_id = False
            pot_auth = sibling.text
            if "gnd/" in pot_auth:
                found_id = pot_auth.replace("gnd/", "(DE-588)")  # GND
            else:
                if "ppn/" in pot_auth:
                    found_id = pot_auth.replace("ppn/", "(DE-627)")  # K10+
            if found_id:
                if name in Additional_Data["authorities"].keys():
                    if found_id not in Additional_Data["authorities"][name]:
                        Additional_Data["authorities"][name].append(found_id)
                else:
                    Additional_Data["authorities"][name] = [found_id]

    (metadata, header, id, metahash, uuid) = Available_Data
    (DataSource, Formats, Relations) = Default_Data

    Relations = Relations[metadatatype]
    # Defaults
    DataDict = {"id": uuid, "collection": DataSource["Collection_Name"], "remote_bool": "true",
                "collection_details": DataSource["Collection_Details"], "up_date": datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")}
    Additional_Data = {}

    if metadatatype in Formats.keys():
        Fulltext = ""
        if metadatatype == "oai_dc":  # Outdated
            pass
        if metadatatype == "xoai_discovery":
            # type
            if metadata.find("element", {"name": "type"}):
                xoaitype = metadata.find("element", {"name": "type"}).find("field", {"name": "value"}).text
            else:
                xoaitype = "unknown-notype-" + str(id)
            if xoaitype in Formats[metadatatype].keys():
                DataDict["format_phy_str_mv"] = Formats[metadatatype][xoaitype]                  
            else:
                DataDict["format_phy_str_mv"] = "unknown"
                write_error_format(xoaitype, metadatatype, id)
            # date
            # Original release
            if metadata.find("element", {"name": "date"}).find("element", {"name": "issued"}):
                DataDict["publishDateSort"] = metadata.find("element", {"name": "date"}).find("element", {"name": "issued"}).find("field").text
            else:
                if metadata.find("element", {"name": "date"}).find("element", {"name": "available"}):
                    ADate = metadata.find("element", {"name": "date"}).find("element", {"name": "available"}).find("field").text
                    Convdate = datetime.datetime.strftime(datetime.datetime.strptime(ADate, "%Y-%m-%dT%H:%M:%SZ"), "%Y-%m-%d")
                    if Convdate:
                        DataDict["publishDateSort"] = Convdate
                    else:
                        DataDict["publishDateSort"] = ADate
                else:
                    for Datum in metadata.find("element", {"name": "date"}).find_all("field"):
                        TakeWhatYouCan = datetime.datetime.strftime(datetime.datetime.strptime(Datum.text, "%Y-%m-%dT%H:%M:%SZ"), "%Y-%m-%d")
                        if TakeWhatYouCan:
                            DataDict["publishDateSort"] = TakeWhatYouCan
                            break
            # Local release
            if metadata.find("element", {"name": "date"}).find("element", {"name": "available"}):
                if metadata.find("element", {"name": "date"}).find("element", {"name": "available"}).find("field"):
                    Fulldate = metadata.find("element", {"name": "date"}).find("element", {"name": "available"}).find("field").text
                    FulldateC = datetime.datetime.strftime(datetime.datetime.strptime(Fulldate, "%Y-%m-%dT%H:%M:%SZ"), "%Y")
                    if Fulldate:
                        Additional_Data["AvailableDate"] = FulldateC
                    else:
                        Additional_Data["AvailableDate"] = Fulldate
            else:
                if metadata.find("element", {"name": "date"}).find("element", {"name": "accessioned"}):
                    Fulldate = metadata.find("element", {"name": "date"}).find("element", {"name": "accessioned"}).find("field").text
                    FulldateC = datetime.datetime.strftime(datetime.datetime.strptime(Fulldate, "%Y-%m-%dT%H:%M:%SZ"), "%Y")
                    if Fulldate:
                        Additional_Data["AvailableDate"] = FulldateC
                    else:
                        Additional_Data["AvailableDate"] = Fulldate
                else:
                    Additional_Data["AvailableDate"] = DataDict["publishDateSort"]
            # author
            Additional_Data["authorities"] = {}

            # persons of interest
            if metadata.find("element", {"name": "contributor"}):
                contributors = metadata.find("element", {"name": "contributor"})
                for contributor in contributors.findChildren(recursive=False):
                    role = contributor.get("name")
                    kidname = ""
                    for kid in contributor.findChildren(name="field", attrs={"name": "value"}):
                        find_authority(kid)
                        kidname = kid.text
                        if role == "author":
                            if "author" not in DataDict.keys():
                                DataDict["author"] = kidname
                            else: # author is a single-value SOLR field, should not matter bc author2
                                if "other_persons" in Additional_Data.keys():
                                    Additional_Data["other_persons"].append((kidname, "author", "aut"))
                                else:
                                    Additional_Data["other_persons"] = [(kidname, "author", "aut")]
                        else:
                            if role in Relations.keys():
                                shortrole = Relations[role]
                            else:
                                write_error_relations(role, metadatatype, id)
                                shortrole = "oth"  # Other
                            if "other_persons" in Additional_Data.keys():
                                Additional_Data["other_persons"].append(
                                    (kidname, role, shortrole))
                            else:
                                Additional_Data["other_persons"] = [(kidname, role, shortrole)]
                        if "author2" in DataDict.keys():
                            DataDict["author2"].append(kidname)
                        else:
                            DataDict["author2"] = [kidname]

            # foreign_ids_str_mv
            Identifiers = []
            for child in metadata.find("element", {"name": "identifier"}).find_all("element"):
                if child.get("name") == "urn":
                    Identifiers.append((child.find("field").text, "URN"))  # URN
                if child.get("name") == "other":
                    for childfield in child.find_all("field"):
                        if "gbv" in childfield.text:
                            Identifiers.append((childfield.text.split(":")[-1], "PPN"))  # PPN
                        if "vd" in childfield.text.lower() and ":" in childfield.text.lower():
                            Identifiers.append((childfield.text.split(":", 1)[-1], "vd" + childfield.text.lower().split("vd")[1][0:2]))  # VD
                        if "fingerprint" in childfield.text and ":" in childfield.text and "|" in childfield.text:
                            Identifiers.append((childfield.text.split(":", 1)[-1].split("|")[-1], "fingerprint"))  # Fingerprint
                if child.get("name") == "ppn":
                    Identifiers.append((child.find("field").text, "PPN"))######
                    PPNCandidate = child.find("field").text
                    if xoaitype == "PeriodicalPart":
                        if metadata.find("element", {"name": "order"}):
                            PPNADD = metadata.find("element", {"name": "order"}).find("field").text
                            PPNCandidate = PPNCandidate.split(PPNADD)[0]
                    Additional_Data["SeeOtherEntry"] = PPNCandidate
                if child.get("name") == "vd":
                    vdnum = child.find("field").text
                    Identifiers.append((vdnum, vdnum[0:4]))
                if child.get("name") == "uri":
                    for childfield in child.find_all("field"):
                        if "doi" in childfield.text:
                            Identifiers.append(
                                (childfield.text, "doi"))  # DOI-URL
                            DataDict["url"] = [childfield.text]
                            DataDict["doi_str_mv"] = childfield.text.split(
                                "doi.org/")[-1]  # Suchraum
                if child.get("name") == "issn":
                    DataDict["issn"] = child.find("field").text  # ISSN
                if child.get("name") == "isbn":
                    DataDict["isbn"] = child.find("field").text  # ISBN
                if child.get("name") == "shelfmark":
                    if "signature" in DataDict.keys():
                        DataDict["signature_iln"].append(
                            child.find("field").text)
                    else:
                        DataDict["signature_iln"] = [child.find("field").text]
            if Identifiers:
                DataDict["foreign_ids_str_mv"] = Identifiers
                # DataDict["url"] = Identifiers
                # DataDict["doi_str"]

            # Norm title
            if metadata.find("element", {"name": "title"}):
                if metadata.find("element", {"name": "title"}).find("element", {"name": "uniform"}):
                    uniform_title = metadata.find("element", {"name": "title"}).find(
                        "element", {"name": "uniform"}).find("field", {"name": "value"})
                    Additional_Data["uniform_title"] = uniform_title.text
                    find_authority(uniform_title)
            # Topics
            Subjects = []
            if metadata.find("element", {"name": "subject"}):
                # DDCs and keywords
                for subject in metadata.find("element", {"name": "subject"}).find_all("field", {"name": "value"}):
                    if subject.parent.parent.get("name") == "ddc":  # ddc
                        if "dewey-full" in DataDict.keys():
                            DataDict["dewey-full"].append(subject.text)
                        else:
                            DataDict["dewey-full"] = [subject.text]
                    Subjects.append(subject.text)
            if Subjects:
                DataDict["topic_facet"] = Subjects
                DataDict["topic_title"] = Subjects
                # DataDict["topic_unstemmed"] = Subjects
                # DataDict["topic"] = Subjects
                # DataDict["topic_browse"] = Subjects
            # Language
            if metadata.find("element", {"name": "language"}):
                DataDict["lang_code"] = metadata.find("element", {"name": "language"}).find("field", {"name": "value"}).text
            # title
            for element in metadata.find_all("element", {"name": "title"}):
                if element.parent.get("name") == "dc":
                    xoaititle = element.find("field").text
                else:
                    pass
                DataDict["title"] = xoaititle
                DataDict["title_short"] = xoaititle
                DataDict["title_full"] = xoaititle
            # parttitle
            for element in metadata.find_all("element", {"name": "title"}):
                if element.parent.get("name") == "part":
                    parttitle = element.find("field").text
                    DataDict["title"] = parttitle + " | " + DataDict["title"]
                    DataDict["title_short"] = parttitle + \
                        " | " + DataDict["title_short"]
                    DataDict["title_full"] = parttitle + \
                        " | " + DataDict["title_full"]
            # Abstract
            if metadata.find("element", {"name": "abstract"}):
                Additional_Data["description"] = []
                # ger and eng
                for abstract in metadata.find("element", {"name": "abstract"}).find_all("field"):
                    Additional_Data["description"].append(abstract.text)  # Only abstract(s), no notes
            # Notes
            NoteList = []
            if metadata.find("element", {"name": "note"}):
                for note in metadata.find("element", {"name": "note"}).find_all("field"):
                    if note.text:
                        NoteList.append(note.text)
            if metadata.find("element", {"name": "comment"}):
                for comment in metadata.find("element", {"name": "comment"}).find_all("field"):
                    if comment.text:
                        NoteList.append(comment.text)
            if NoteList:
                Additional_Data["notes"] = NoteList
            # License
            if metadata.find("element", {"name": "rights"}):
                Additional_Data["License"] = metadata.find("element", {"name": "rights"}).find("field", {"name": "value"}).text

            # Journal
            if metadata.find("element", {"name": "bibliographicCitation"}):
                BibCit = metadata.find(
                    "element", {"name": "bibliographicCitation"})
                if BibCit.find("element", {"name": "journaltitle"}):
                    BibCitJournalTitle = BibCit.find("element", {"name": "journaltitle"})
                    DataDict["journal"] = BibCitJournalTitle.find("field", {"name": "value"}).text
                    DataDict["journalStr"] = DataDict["journal"]
                    if BibCit.find("element", {"name": "volume"}):
                        BibCitVolume = BibCit.find("element", {"name": "volume"})
                        Additional_Data["journal_volume"] = BibCitVolume.find("field", {"name": "value"}).text
                        if BibCit.find("element", {"name": "pagestart"}):
                            BibCitPagestart = BibCit.find("element", {"name": "pagestart"})
                            Additional_Data["journal_pagestart"] = BibCitPagestart.find("field", {"name": "value"}).text
            # Relations
            if metadata.find("element", {"name": "relation"}):
                Additional_Data["relations"] = []
                for relfield in metadata.find("element", {"name": "relation"}).find_all("field"):
                    if "http" in relfield.text.lower():  # HTTP and HTTPS
                        Additional_Data["relations"].append(relfield.text)
                    else:
                        if relfield["name"] == "value":
                            DataDict["series"] = relfield.text
                if not Additional_Data["relations"]:
                    del Additional_Data["relations"]
            # Publisher
            DataDict["publisher"] = [DataSource["Publisher"]]
            if metadata.find("element", {"name": "publisher"}):
                Additional_Data["publisher_add"] = []
                for pub in metadata.find_all("element", {"name": "publisher"}):
                    # Repo and local publisher need to be in publisher, but repo needs ind1 = 2 (distributor)
                    DataDict["publisher"].append(pub.find("field", {"name": "value"}).text)
                    Additional_Data["publisher_add"].append(pub.find("field", {"name": "value"}).text)
                # Same publisher as contributor and publisher
                Additional_Data["publisher_add"] = list(set(Additional_Data["publisher_add"]))
                DataDict["publisher"] = list(set(DataDict["publisher"]))

            # Form
            if metadata.find("element", {"name": "genre"}):
                DataDict["genre_facet"] = []
                for genrefield in metadata.find("element", {"name": "genre"}).find_all("field"):
                    DataDict["genre_facet"].append(genrefield.text)
                    
            # Extent
            if metadata.find("element", {"name": "extent"}):
                DataDict["physical"] = metadata.find(
                    "element", {"name": "extent"}).find("field").text
            # Edition
            if metadata.find("element", {"name": "edition"}):
                DataDict["edition"] = metadata.find(
                    "element", {"name": "edition"}).find("field").text
            # Conference
            if metadata.find("element", {"name": "conference"}):
                conf = metadata.find("element", {"name": "conference"})
                if conf.find("element", {"name": "name"}):
                    Additional_Data["conference"] = conf.find(
                        "element", {"name": "name"}).find("field").text
                if conf.find("element", {"name": "place"}):
                    Additional_Data["conference_place"] = conf.find(
                        "element", {"name": "place"}).find("field").text
            # Fulltext
            for bundle in metadata.find_all("element", {"name": "bundle"}):
                if "TEXT" == bundle.find("field", {"name": "name"}).text:
                    fulltexturl = bundle.find("field", {"name": "url"}).text
                    raw_fulltext = str(requests.get(fulltexturl, headers={"User-Agent": "ulbbot+myk10harvest_txt"}).content.decode("utf-8"))
                    Fulltext = raw_fulltext.replace("\n", "")
            # Open Access
            if metadata.find("element", {"name": "openaccess"}):
                Additional_Data["openaccess"] = metadata.find("element", {"name": "openaccess"}).find("field", {"name": "value"}).text

            # Mets?
            if metadata.find("element", {"name": "mets"}):
                Additional_Data["mets"] = metadata.find("element", {"name": "mets"}).find("field", {"name": "value"}).text
                for child in metadata.find("element", {"name": "identifier"}).find_all("element"):
                    if child.get("name") == "urn":
                        Additional_Data["mirador"] = "https://nbn-resolving.de/" + child.find("field").text
                


    else:
        with open(Errorlog, "a") as file:
            file.write(str(datetime.datetime.now()) + "\n")
            file.write("Metadatatype was not found in Format File:\n")
            file.write(str(metadatatype) + "\n")
            file.write("The script will now stop." + "\n" + "\n")
        sys.exit()

    allfields = ""
    for key in DataDict.keys():
        allfields = allfields_calc(allfields, DataDict[key])

    for key in Additional_Data.keys():
        allfields = allfields_calc(allfields, Additional_Data[key])

    DataDict["allfields"] = allfields
    DataDict["fulltext"] = Fulltext
    DataDict["fullrecord_marcxml"] = str(get_marc(Available_Data, DataDict, Additional_Data, DataSource, Marc_Folder))

    write_new_file(DataDict, New_Folder, Debuglevel, Logfile)
