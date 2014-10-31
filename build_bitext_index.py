#Usage: python read_csv [delimiter] [index] [typekey]
import csv
import sys
import requests
import json
from datetime import datetime

filename = sys.argv[1]
delimiter = ";"
index = "bitext"
type_key = "section"
if len(sys.argv) > 2:
    delimiter = sys.argv[2]
if len(sys.argv) > 3:
    index = sys.argv[3]
if len(sys.argv) > 4:
    type_key = sys.argv[4]

with open(filename) as csv_file:
    reader = csv.reader(csv_file, delimiter=delimiter)
    keys = reader.next()
    for _id,line in enumerate(reader):
        result = {}
        for i, value in enumerate(line):
            result[keys[i]] = value
        result["score"] = float(result["score"].replace(",","."))
        #result["date"] = datetime.strptime(result["date"], '%d/%m/%Y %H:%M:%S')
        query = {
            "doc":result,
            "doc_as_upsert":True
        }
        response = requests.post("http://localhost:9200/" + index + "/" + result[type_key] + "/" + str(_id) + "/_update", data=json.dumps(query))
        print(response.text)
