#Usage: just type python add_hotels.py
#this script iterates over the bitext index in elasticserach and adds the hotel information stored in the hotels index
import requests
import json
#query matching all
query = {
	"query":{
		"match_all":{}
	}
}
#scroll size
scroll_size = 100
#launch scroll request
scroll_response = json.loads(requests.get("http://localhost:9200/bitext_unique/_search?search_type=scan&scroll=1m&size=" + str(scroll_size), data=json.dumps(query)).text)
#read the first elements
response = json.loads(requests.get("http://localhost:9200/_search/scroll?scroll=1m", data=scroll_response["_scroll_id"]).text)
hits = response["hits"]["hits"]
print("hits: " + str(len(hits)))
while len(hits):
	for bitext_unique_element in hits:
		#print(bitext_element)
		#search in hotels
		comments_response = json.loads(requests.get("http://localhost:9200/comments/_all/" + bitext_unique_element["_id"]).text)
		if comments_response["found"]:
			update_doc = {
				"averageWebScore":comments_response["_source"]["averageWebScore"]
			}
			#update_doc["hotelName"] = hotels_response["_source"]["hotelName"]
			#update_doc["destinationCode"] = hotels_response["_source"]["destinationCode"]
			update_response = requests.post("http://localhost:9200/" + bitext_unique_element["_index"] + "/" + bitext_unique_element["_type"] + "/" + bitext_unique_element["_id"] + "/_update", data=json.dumps({"doc":update_doc}))
			print(update_response.text)
		else:
			print("Comment not found: " + bitext_unique_element["_id"])
	#load the next batch of elements
	response = json.loads(requests.get("http://localhost:9200/_search/scroll?scroll=1m", data=scroll_response["_scroll_id"]).text)
	hits = response["hits"]["hits"]
	print("hits: " + str(len(hits)))
