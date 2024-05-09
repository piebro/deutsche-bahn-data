# Deutsche Bahn Data

This is a repository with accumulated public data from "Deutsche Bahn", the biggest german train company.

## Data Gathering

### Die größten Bahnhöfe ermitteln

Die einfachste Möglichkeit die größten Bahnhöfe Deutschlands zu bekommen ist über die [Preisklasse](https://de.wikipedia.org/wiki/Preisklasse). Diese gibt indirekt an wie groß ein Bahnhof ist. Dazu habe ich eine [aktuelle Tabelle](https://www.deutschebahn.com/resource/blob/11895816/ef4ecf6dd8196c7db3ab45609d8a2034/Stationspreisliste-2024-data.pdf) aller Bahnhöfe mit ihren Preisklassen gefunden. Das Problem ist das ich noch die eva Nummer der Bahnhöfe brauche für die API.

https://wiki.openstreetmap.org/w/images/c/c2/20141001_IBNR.pdf (Daten von 1.10.2014) die Zuweisung von dem Namen des ahnhofs zu ihrer IBNR-Nummer (in der api heißt wir die nummer eva gennant). Die API woher die Daten kommen gibt es nicht mehr (https://data.deutschebahn.com/dataset/data-haltestellen), daher wird eine alte Versionhier benutzt. In ihr habe ich aber die Nummern für alle relevanten Bahnhöfe gefunden.

Diese beiden Datenquellen werden in `save_eva_name_list.py` benutzt um eine Liste der ~100 größten Bahnhöfe Deutschlands mit Name und eva Nummer zu erstellen.

Dies sind die Befehle um die Liste selber zu erstellen:

```bash
# download the two pdfs with the data
wget https://www.deutschebahn.com/resource/blob/11895816/ef4ecf6dd8196c7db3ab45609d8a2034/Stationspreisliste-2024-data.pdf
wget https://wiki.openstreetmap.org/w/images/c/c2/20141001_IBNR.pdf

# install dependancies for the script
pip3 install tabula-py PyPDF2

# run the script
python3 save_eva_name_list.py
```

Wenn jemand eine aktuelle Liste von Bahnhöfen und ihrer eva nummer findet erstellt gerne ein issue.



https://editor.swagger.io/