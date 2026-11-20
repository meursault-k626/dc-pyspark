#!/usr/bin/bash

for url in \
    "https://vision.stanford.edu/aditya86/ImageNetDogs/annotation.tar" \
    # "https://assets.datacamp.com/production/repositories/4336/datasets/f412f5acbef38630147c2956d8703bafbcd0f74c/AA_DFW_2014_Departures_Short.csv.gz" \
    # "https://assets.datacamp.com/production/repositories/4336/datasets/c0aa672020bce21eef0d875a484a4fd44da042cf/DallasCouncilVoters.csv.gz" \
    # "https://assets.datacamp.com/production/repositories/4336/datasets/ea700976560a6f1760782c6e7310a662120c63b5/DallasCouncilVotes.csv.gz" \
    # "https://assets.datacamp.com/production/repositories/4336/datasets/c1abacafea802998597d6c68b27c7b8650a18ab8/AA_DFW_2016_Departures_Short.csv.gz" \
    # "https://assets.datacamp.com/production/repositories/4336/datasets/04db01ffbd39f7bf2f88ffd5b7924b2de0419168/AA_DFW_2017_Departures_Short.csv.gz" \
    # "https://assets.datacamp.com/production/repositories/4336/datasets/475d2803541ba8facb2c39024dd0d9497859dc6c/AA_DFW_2015_Departures_Short.csv.gz"
do
    curl -fLO "$url"
done

# cp AA_DFW_2017_Departures_Short.csv.gz AA_DFW_2018.csv.gz

tar -xvf annotation.tar

# Flatten the Stanford Dogs XML annotations into a tab-delimited annotations.csv.gz,
# injecting junk rows to mirror the messy DataCamp exercise file.
python3 - <<'PY'
import os, gzip, random
import xml.etree.ElementTree as ET

ANNOT_DIR = "Annotation"
OUTPUT = "annotations.csv.gz"
random.seed(42)

def flatten(path):
    root = ET.parse(path).getroot()
    folder   = root.findtext("folder", "")
    filename = root.findtext("filename", "")
    size     = root.find("size")
    width    = size.findtext("width", "")  if size is not None else ""
    height   = size.findtext("height", "") if size is not None else ""

    dogs = []
    for obj in root.findall("object"):
        name = obj.findtext("name", "")
        box  = obj.find("bndbox")
        coords = [box.findtext(k, "") for k in ("xmin", "ymin", "xmax", "ymax")]
        dogs.append(",".join([name] + coords))   # breed,xmin,ymin,xmax,ymax

    # folder \t filename \t width \t height \t dog1 \t dog2 ...
    return "\t".join([folder, filename, width, height] + dogs)

rows = []
for dirpath, _, files in os.walk(ANNOT_DIR):
    for fn in files:
        try:
            rows.append(flatten(os.path.join(dirpath, fn)))
        except ET.ParseError:
            continue  # skip anything that isn't valid XML

# --- inject junk rows to mirror the DataCamp exercise file ---
# 1. '#' comment lines (for the comment='#' exercise)
comment_rows = [f"# {random.randint(100, 999)}" for _ in range(40)]
# 2. short 2-field rows: folder + MISMATCHED filename, <5 fields (for the field-count filter)
short_rows = [
    f"0{random.randint(2000000, 29999999)}\tn0{random.randint(20000000, 29999999)}_{random.randint(1, 9999)}"
    for _ in range(25)
]
# 3. '%s' template rows: 5 fields, so a field-count filter alone won't catch them
template_rows = [
    f"%s\t%s\t{random.choice([800, 640, 500])}\t{random.choice([600, 480, 375])}\tShetland_sheepdog,124,87,576,514"
    for _ in range(5)
]

junk = comment_rows + short_rows + template_rows
for row in junk:
    rows.insert(random.randint(0, len(rows)), row)   # scatter, don't clump at the top

with gzip.open(OUTPUT, "wt") as f:
    f.write("\n".join(rows) + "\n")

print(f"Wrote {len(rows)} rows -> {OUTPUT}")
print(f"  injected {len(comment_rows)} '#' rows, "
      f"{len(short_rows)} short rows, {len(template_rows)} '%s' rows")
PY


rm -r Annotation
# for f in *.gz; do
#     gunzip -kf "$f"
# done