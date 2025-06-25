import requests
from bs4 import BeautifulSoup


# Check if record is already in catalog
def check_catalog(metadata, metadatatype, catalogcheck_needed, Lists_PPN):
    own_doi = "10.25673/" # Change as needed
    own_url = "uni-halle.de/" # Change as needed
    def request_catalog_title(title):
        cleantitle = ""

        for char in title:
            if char.isalnum():
                cleantitle = cleantitle + char
            else:
                cleantitle = cleantitle + " "
        Katalogrequest = "https://sru.k10plus.de/k10plus!levels=0?version=1.1&operation=searchRetrieve&query=pica.all='" + \
            str(cleantitle) + "'&maximumRecords=10&recordSchema=dc"
        kr = requests.get(Katalogrequest)
        kr.encoding = "UTF-8"
        ksoup = BeautifulSoup(kr.text, features="xml")
        for krecord in ksoup.find_all("zs:record"):
            for kident in krecord.find_all("dc:identifier"):
                if "doi" in kident.text:
                    if own_doi in kident.text:
                        return "K10+-DOI\n" + kident.text
                else:
                    if own_url in kident.text:
                        return "K10+-URL\n" + kident.text
        Katalogrequest = "https://sru.k10plus.de/k10plus!levels=0?version=1.1&operation=searchRetrieve&query=pica.all='" + \
            str(title) + "'&maximumRecords=10&recordSchema=dc"
        kr = requests.get(Katalogrequest)
        kr.encoding = "UTF-8"
        ksoup = BeautifulSoup(kr.text, features="xml")
        for krecord in ksoup.find_all("zs:record"):
            for kident in krecord.find_all("dc:identifier"):
                if "doi" in kident.text:
                    if own_doi in kident.text:
                        return "K10+-DOI\n" + kident.text
                else:
                    if own_url in kident.text:
                        return "K10+-URL\n" + kident.text
        return False

    def request_catalog_urn(urn):
        if urn:
            clean_urn = ""
            for letter in urn:
                if letter.isalnum():
                    clean_urn = clean_urn + letter
            URNRequest = "https://sru.k10plus.de/k10plus?version=1.1&operation=searchRetrieve&query=pica.url=" + \
                str(clean_urn) + "&maximumRecords=10&recordSchema=dc"
            ur = requests.get(URNRequest)
            usoup = BeautifulSoup(ur.text, features="xml")
            if usoup.find("zs:record"):
                return "K10+-URN\n" + clean_urn
            else:
                URN_n_Request = "https://sru.k10plus.de/k10plus?version=1.1&operation=searchRetrieve&query=pica.url=nbnresolving.de" + \
                    str(clean_urn) + "&maximumRecords=10&recordSchema=dc"
                urn = requests.get(URN_n_Request)
                usoup = BeautifulSoup(urn.text, features="xml")
                if usoup.find("zs:record"):
                    return "K10+-URN-nbn\n" + clean_urn
        return False

    def ppncheck(ppncandidate, in_catalog, Lists_PPN):
        (Allowlist_PPN, Denylist_PPN) = Lists_PPN
        if ppncandidate in Allowlist_PPN:
            return (False, 1)
        if ppncandidate in Denylist_PPN:
            return ("Denylist-" + ppncandidate, 0)
        if not in_catalog:
            in_catalog = ppncandidate
        return (in_catalog,)

    in_catalog = False
    if (not metadata):  # Deleted file
        return in_catalog
    urn = ""
    if metadatatype == "xoai_discovery": # Dependend on the available metadatatype
        element = metadata.find("element", {"name": "identifier"})
        catalog_response = [False]
        for child in element.find_all("element"):
            if child.get("name") == "urn":
                urn = child.find("field").text
            if child.get("name") == "ppn":
                ppncandidate = child.find("field").text
                catalog_response = ppncheck(ppncandidate, in_catalog, Lists_PPN)
        if len(catalog_response) > 1: # Lists are more important than catalogcheck_needed
            (in_catalog, allowed) = catalog_response
            return in_catalog
        else:
            if catalogcheck_needed == 0:
                return False
            else:
                if catalogcheck_needed == 2:
                    return "Catalogcheck_Allowlist_only"
                else:
                    if catalogcheck_needed == 1:
                        in_catalog = catalog_response[0]
                        if not in_catalog:
                            if urn:
                                in_catalog = request_catalog_urn(urn)
                                if not in_catalog:
                                    for element in metadata.find_all("element", {"name": "title"}):
                                        if element.parent.get("name") == "dc":
                                            in_catalog = request_catalog_title(
                                                element.find("field").text)
                        return in_catalog

                    else:
                        return False
                        # Better to upload dupes than nothing
