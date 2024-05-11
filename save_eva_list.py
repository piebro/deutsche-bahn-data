import tabula
import PyPDF2

# get biggest station using the "Preisklasse"
tables = tabula.read_pdf("Stationspreisliste-2024-data.pdf", pages='all')
biggest_stations = []
for df in tables:
    for _, row in df[df["Preis-\rklasse"] <= 2].iterrows():
        biggest_stations.append(row["Bahnhof"].replace("-", " ").replace(" Hauptbahnhof", " Hbf"))

# read IBNR/eva PDFs as text
with open('20141001_IBNR.pdf', 'rb') as file:
    reader = PyPDF2.PdfReader(file)
    num_pages = len(reader.pages)
    IBNR_text = ""

    for i in range(num_pages):
        page = reader.pages[i]
        IBNR_text += page.extract_text() + "\n"

# create name and eva list and do some sting manupulations to match the different names in the PDFs
eva_list = []
for eva_name in IBNR_text.split("\n")[2:-1]:
    eva_name_split = eva_name.split(" ")
    name = " ".join(eva_name_split[:-1])
    # TODO: do better fuzzy matching of the names
    if name == "Ludwigshafen(Rh)Hbf":
        name = "Ludwigshafen (Rhein) Hbf"
    elif name == "Berlin Brandenburg Flughafen":
        name = "Flughafen BER"
    else:
        name = name.replace("-", " ").replace("(", " (").replace(")", ") ").replace(")  ", ") ").rstrip()
    
    eva = eva_name_split[-1]
    if eva == "08005589": # this is an error, there are two "Solingen Hbf"
        continue
    
    if (name in biggest_stations) or (name.replace(" Hbf", "") in biggest_stations) or (f"{name} Hbf" in biggest_stations):
        eva_list.append(eva)
    

with open("eva_name_list.txt", "w") as f:
    f.write("\n".join(eva_list))