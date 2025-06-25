# Information
Lade Daten von OAI-Schnittstellen und wandle sie in exportbereite `.json`-Dateien um (`Harvest.py`), die in den MyK10+ importiert werden können (`Solr_Export.py`).
# Initiales Setup

1. `python3 -m venv venv`

2. `source venv/bin/activate`

3. `pip install -r requirements.txt`

4. Folgende Dateien **müssen** an die eigene Schnittstelle angepasst oder erstellt werden:
    * [Settings/Settings.json](#settingsjson) (oder eigene Settings-Datei erstellen)
    * [Lists/Formats.json](#formatsjson)
    * [lib/Metadatahandling.py](#libmetadatahandling.py) (Komplex)
    * [lib/Catalog.py](#libcatalog.py)

# Start
Falls die Datei `Settings.json` genutzt werden soll:

`source venv/bin/python Harvest.py`

Falls eine andere Settings-Datei genutzt werden soll:

`source venv/bin/python Harvest.py Pfad/Zur/Settingsdatei`

# Dateienbeschreibung
## Harvest.py
Das eigentliche Programm für den Harvest.
## Solr_Export.py
Exportiert erzeugte `.json` in einen Findex-Solr.
## lib/Metadatahandling.py
Alles, was die Behandlung der Metadaten betrifft, ist in diesem Untermodul ausgelagert. Angepasst werden muss die Funktion "gather_metadata" - sie sollte erweitert werden um den eigenen Metadatentyp (ab Zeile 419). "xoai_discovery" ist der Metadatatyp, der von der ULB genutzt ist. Für möglichst wenig Aufwand sollten die DataDict- und AdditionalData-Schlüssel so beibehalten werden (sonst muss die Funktion "get_marc" entsprechend angepasst werden, siehe [JSON-Erzeugung](#json-erzeugung))

## lib/Catalog.py
Alles, was Anfragen an die SRU-Schnittstelle des K10+ betrifft, ist in diesem Untermodul ausgelagert.
## Settings.json
Diese Datei enthält alle Einstellungen für `Harvest.py` und `Solr_Export.py`. Durch ein Kommandozeilenargument kann eine anders benamte `Settings.json` genutzt werden (siehe [Start](#start)).
### Database
Name der lokalen Datenbankdatei
### Debuglevel
* 0: Keine Logdatei wird erzeugt
* 1: Log pro Seite wird erzeugt
* 2: Log pro Record wird erzeugt
* 3: Ausführlicher Log pro Record wird erzeugt

Fehlermeldungen werden immer geloggt.
### New_Folder
Pfad zum Ordner für Dateien, die in den MyK10+ importiert werden sollen. Falls der Ordner nicht vorhanden ist, wird er erstellt.
### Delete_Folder
Pfad zum Ordner für Dateien, die aus dem MyK10+ gelöscht werden sollen. Falls der Ordner nicht vorhanden ist, wird er erstellt.
### InMyK10_Folder
Pfad zum Ordner für Dateien, die aktuell im MyK10+ enthalten sind. Falls der Ordner nicht vorhanden ist, wird er erstellt.
### Logs_Folder
Pfad zum Ordner für Dateien, in welchem die Logs gespeichert werden sollen. Falls der Ordner nicht vorhanden ist, wird er erstellt.
### MyK10_Deleted_Folder
Pfad zum Ordner für Dateien, die früher im MyK10+ enthalten waren, jetzt aber wieder entfernt wurden. Falls der Ordner nicht vorhanden ist, wird er erstellt.
### Logfile
Name der Datei, die den allgemeinen Ablauf des Skripts `Harvest.py` loggt. Diese Datei wird in einem Unterordner in [Logs_Folder](logsfolder) gespeichert, der den Startzeitpunkt und Namen des Skripts erhält. 
### Errorlog
Name der Datei, die die Fehler der Skripte loggt. Diese Datei wird in einem Unterordner in [Logs_Folder](logsfolder) gespeichert, der den Startzeitpunkt und Namen des Skripts erhält. 
### SOLR_Log
Name der Datei, der den Ablauf des Skripts `Solr_Export` loggt. Diese Datei wird in einem Unterordner in [Logs_Folder](logsfolder) gespeichert, der den Startzeitpunkt und Namen des Skripts erhält. 
### Allowlist_PPN
Pfad zu einer Textdatei, die zeilenweise PPNs enthält, die unabhängig vom Katalogstatus hochgeladen werden sollen.
### Denylist_PPN
Pfad zu einer Textdatei, die zeilenweise PPNs enthält, die unabhängig vom Katalogstatus nicht hochgeladen werden sollen.
### Format_File
Pfad zu der Datei, die für die Umwandlung des Record-Types in einen Typ, den MyK10+ akzeptiert, verantwortlich ist. Es muss eine .json-Datei vorliegen.

Für weitere Informationen siehe [Formats.json](#formatsjson)
### Relations_File
Pfad zu der Datei, die für die Umwandlung der Beziehungskennzeichen in eine Form, die der MyK10+ akzeptiert, verantwortlich ist. Es muss eine .json-Datei vorliegen.
### SOLR_Base
Direktlink zur Solr-Schnittstelle des MyK10+
### ID_Prefix
Präfix für die von Lukida genutzten IDs.
### Marc_Folder
Ordner, in welchem MarcXML-Daten gespeichert werden. Siehe auch: [Keep_Marc](#keep_marc)
### Keep_Marc
"0", falls [Marc_Folder](#marc_folder) nach durchlaufen des Skriptes gelöscht werden soll, sonst bleibt der Ordner erhalten (z.B. zum Debuggen, in der Regel nicht notwendig)
### DataSources
Liste von OAI-Schnittstellen, die geharvested werden sollen. Jede Datenquelle braucht folgende Einträge:
#### BaseURL
Link zur OAI-Schnittstelle, dem nur noch der ResumptionToken fehlt.
#### MetadataType
Kontext, der an der OAI-Schnittstelle abgerufen werden soll.
#### Refresh_Days
Die OAI-Schnittstelle wird begrenzt auf Records mit Änderungen in den letzten `Refresh_Days` Tagen. Wird das Skript also täglich ausgeführt, ist "1" sinnvoll, bei wöchentlicher Ausführung "7". Dadurch wird die benötigte Zeit extrem verringert. Dieser Wert ist nur relevant, falls schon einmal die komplette Schnittstelle geharvested wurde, sonst werden alle Records betrachtet. Für weitere Informationen siehe [LastModified](#lastmodified).
#### Collection_Name
Name der Collection, die Records von dieser OAI-Schnittstelle haben soll.
#### Publisher
Name des Publishers, der Ressourcen des Repositoriums zugeordnet werden soll ([M264a](https://www.loc.gov/marc/bibliographic/bd264.html)).
#### Collection_Details
Liste von Werten, die bei Lukida in `collection_details` stehen sollen. Hat Einfluss auf die Anzeige und sollte alle ILN enthalten, deren Exemplare angezeigt werden sollen.
#### Full_Refresh
Anzahl an Tagen, nach welchen die Datenquelle noch einmal vollständig überprüft werden soll, um gelöschte Datensätze zu finden.
#### Collection_Handles
Liste von Handles, die auf Sammlungen zeigen. Falls nicht leer, werden nur Handles genutzt, die hier drin stehen.
#### Catalog_Check
Soll eine Katalogüberprüfung der Ressource stattfinden?
* 0: Nein
* 1: Ja
* 2: Nur PPNs, die in Allowlist_PPN stehen
## Formats.json
Enthält Formatanpassungen vom Kontext der OAI-Schnittstelle zu einem MyK10+-kompatiblen Format. Weitere Formate können als neue Schlüssel direkt auf der ersten Ebene eingefügt werden.
Falls ein Format an der OAI-Schnittstelle geliefert wird, was nicht in Formats.json gelistet ist, wird eine neue Datei [Formats.txt](#formatstxt) angelegt, die die nicht gefundenen Formate auflistet.
## Formats.txt
Falls ein Format an der OAI-Schnittstelle geliefert wird, was nicht in Formats.json gelistet ist, wird diese Datei angelegt, um die nicht gefundenen Formate aufzulisten. Dabei kann es zu Dopplungen kommen, für jedes Format, was nicht gefunden wird, wird eine Zeile angelegt. Das Skript weist dem Record in diesem Fall das Format "unknown" zu. Die Datei wird im [Logs_Folder](logsfolder) gespeichert.

## Database.db
Name kann variieren, je nach dem, was in [Settings.json](#database) angegeben wurde.

Die lokale Datenbank. Sie enthält drei Tabellen:
### Metadata
Besteht aus zwei Spalten:
* Meta
* Data

`Meta` enthält die Art des Datums, die in `Data` gespeichert wird.
#### Settings
`Settings` ist die [Settings.json](#settingsjson), die bei der Erzeugung dieser Datei genutzt wurde. 
#### LastRun_Duration
Die Anzahl der Sekunden, die die letzte Ausführung des Skriptes dauerte. Ist das Skript nicht vollständig durchgelaufen, steht hier `0`.
#### LastRun_TotalRecords
Die Anzahl an Records, die bei der letzten Ausführung des Skriptes betrachtet wurden. Ist das Skript nicht vollständig durchgelaufen, steht hier `0`.
### Url_Settings
Besteht aus drei Spalten:
* BaseURL
* LastModified
* LastFull

Es wird eine Zeile pro `DataSource` aus [Settings.json](#settingsjson) angelegt.
#### BaseURL
Enthält die BaseURL der `DataSource`.
#### LastModified
Enthält das zur `BaseURL` beim letzten Durchlauf maximale, gefundene `LastModified`-Datum. Dieses Datum, verringert um [$Refresh_Days](#refresh_days) Tage, wird an der OAI-Schnittstelle abgefragt, um den Suchraum nur auf seit dem letzten Durchlauf veränderte Records zu beschränken.
#### LastFull
Enthält das Datum des letzten vollständigen Harvests der Datenquelle. Wird in Zusammenhang mit [Full_Refresh](#full_refresh) benötigt.
### Records
Besteht aus fünf Spalten:
* Identifier
* Metahash
* UUID
* Status
* MyK10

#### Identifier
Identifier des Records, wie er an der OAI-Schnittstelle ausgegeben wird.
#### Metahash
SHA-256 gehashte Metadaten des Records.
#### UUID
Eigens erzeugte ID für den Record, unabhängig von anderen Werten. Entspricht der Form [ID_Prefix](id_prefix) + hexadecimal(Laufende Nummer). Die laufende Nummer wird in der Datenbank gespeichert und vor jeder Erzeugung hochgesetzt, damit die Eindeutigkeit in jedem Fall erhalten bleibt. Mit der aktuellen Methode können nicht mehr als $68719476735 = 2^{36}-1$ UUIDs erstellt werden.
#### Status
Ob der Record an der OAI-Schnittstelle gelöscht oder vorhanden war.
#### MyK10
1 falls der Record im MyK10 seien müsste, 0 sonst.
# Logiken
## Katalog
Falls geprüft wird, ob ein Metadatensatz bereits im Katalog vorhanden ist, werden mehrere Tests durchgeführt:
### PPN-Test
Es wird überprüft, ob im Metadatensatz eine PPN gefunden wird. Ist das der Fall wird davon ausgegangen, dass der Record im K10+ vorhanden ist, er wird also nicht in den MyK10+ exportiert.

### URN-Test
Ist keine PPN gefunden worden, und wenn eine URN existiert, wird eine Anfrage an die SRU-Schnittstelle des K10+ gestellt, die nach der URN sucht. Wird hier mindestens ein Record gefunden, wird davon ausgegangen, dass der Record im K10+ vorhanden ist, er wird also nicht in den MyK10+ exportiert.
### DOI-Test
Wurde noch nicht festgestellt, ob der Record im K10+ vorhanden ist, werden die Identifier des Records betrachtet. Steht in einem Identifier `uni-halle.de/`, wird davon ausgegangen, dass die Datei einem unserer Repositorien liegt, der Record wird also nicht in den MyK10+ exportiert. Es wird auch geschaut, ob `doi` in einem Identifier steht, und wenn ja, wird geschaut, ob `10.25673/`auch in diesem Identifier steht. Sowohl Identifier als auch DOI-Präfix können in [Catalog.py](#libcatalog.py) angepasst werden. In diesem Fall verlinkt der Katalogeintrag auf eines unserer Repositorien, es gibt also keinen Bedarf, den Record in den MyK10+ zu exportieren.
### Ergebnis
Sind alle vorigen Tests fehlgeschlagen, wird davon ausgegangen, dass der Record nicht im K10+ vorhanden ist, oder, falls er vorhanden ist, kein Link zu unseren Repositorien existiert. Er wird also in den MyK10+ exportiert. Dadurch kann es zu doppelten Ergebnissen kommen, da z.B. Original- und Zweitveröffentlichung aufgelistet werden.

## JSON-Erzeugung
Abhängig vom Metadatentyp werden die Metadaten nach den gewünschten Daten durchsucht. Ist der Metadatentyp nicht in [Formats.json](#formatsjson) definiert, wird das Programm beendet. Die Daten für die äußere JSON werden in `DataDict` gespeichert, Daten, die nur für die MarcXML-Erzeugung benötigt werden, werden in `Additional_Data` gespeichert. Wurden alle gewünschten Metadaten abgearbeitet, werden `DataDict` und `Additional_Data` genutzt, um die MarcXML zu erzeugen. Da die Metadaten vorher metadatentypabhängig extrahiert wurden, ist durch diese Vorgehensweise die Erzeugung des MarcXML metadatentypunabhängig.

## Lokale Datenbank: Records
Da sich der Identifier bei Umzug eines Records ändert, ist dieser keine gute eindeutige Identifizierung. Um einen umgezogenen Record wiederzufinden, werden die Metadaten gehasht - da sich die Metadaten durch einen Umzug nicht ändern, kann der Record so wieder gefunden werden. Um den Record im MyK10+ eindeutig zu identifizieren, wird eine UUID angelegt, welche als "id" im MyK10+ genutzt wird, sowie als Dateiname für die `.json` des Records. Der Status wird gespeichert, um auf neu gelösche Records reagieren zu können. Der MyK10+-Status wird gespeichert, um unnötige Anfragen an die SRU-Schnittstelle des MyK10+ zu vermeiden.