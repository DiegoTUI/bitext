#Usage: python read_csv filename [skip_lines] [delimiter] [index] [typekey]
import csv
import sys
import requests
import json

filename = sys.argv[1]
delimiter = ";"
index = "comments"
type_key = "hotelSequence"
skip_lines = 0
if len(sys.argv) > 2:
    skip_lines = int(sys.argv[2])
if len(sys.argv) > 3:
    delimiter = sys.argv[3]
if len(sys.argv) > 4:
    index = sys.argv[4]
if len(sys.argv) > 5:
    type_key = sys.argv[5]

with open(filename) as csv_file:
    reader = csv.reader(csv_file, delimiter=delimiter)
    keys = reader.next()
    # skip lines
    all(reader.next() for i in range(skip_lines))
    for line in reader:
        result = {}
        for i, value in enumerate(line):
            result[keys[i]] = value.translate(None, ".")
	try:
		result["averageWebScore"] = int(result["averageWebScore"])
	except:
		result["averageWebScore"] = 0
        _id = result["commentId"]
	del result["commentId"]
        query = {
            "doc":result,
            "doc_as_upsert":True
        }
        response = requests.post("http://localhost:9200/" + index + "/" + result[type_key] + "/" + str(_id) + "/_update", data=json.dumps(query))
        print(response.text)
