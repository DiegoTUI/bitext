#Usage: python read_csv filename [delimiter] [index] [typekey]
import csv
import sys
import requests
import json
from datetime import datetime

filename = sys.argv[1]
delimiter = ";"
index = "hotels"
type_key = "destinationCode"
if len(sys.argv) > 2:
    delimiter = sys.argv[2]
if len(sys.argv) > 3:
    index = sys.argv[3]
if len(sys.argv) > 4:
    type_key = sys.argv[4]

with open(filename) as csv_file:
    reader = csv.reader(csv_file, delimiter=delimiter)
    keys = reader.next()
    for line in reader:
        result = {}
        for i, value in enumerate(line):
            result[keys[i]] = value.translate(None, ".")
	    if i>2:
	    	result[keys[i]] = int(result[keys[i]])
        _id = result["hotelSequence"]
	del result["hotelSequence"]
        query = {
            "doc":result,
            "doc_as_upsert":True
        }
        response = requests.post("http://localhost:9200/" + index + "/" + result[type_key] + "/" + str(_id) + "/_update", data=json.dumps(query))
        print(response.text)
