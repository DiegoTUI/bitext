#Usage: just type python add_hotels.py
#this script iterates over the bitext index in elasticserach and creates a new index with unique comment ids
#each comment_id will be suffixed with POS or NEG to distinguish between positive and negative comments
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
scroll_response = json.loads(requests.get("http://localhost:9200/bitext/_search?search_type=scan&scroll=1m&size=" + str(scroll_size), data=json.dumps(query)).text)
#read the first elements
response = json.loads(requests.get("http://localhost:9200/_search/scroll?scroll=1m", data=scroll_response["_scroll_id"]).text)
hits = response["hits"]["hits"]
print("hits: " + str(len(hits)))
while len(hits):
	for bitext_element in hits:
		#previous score
		previous_average_score = 0
		previous_count = 0
		previous_categories = ""
		separator = ""
		#check if the commentId already exists in bitext_unique
		bitext_unique_key = bitext_element["_source"]["commentId"] + bitext_element["_source"]["section"]
		response_bitext_unique_posneg = json.loads(requests.get("http://localhost:9200/bitext_unique_posneg/_all/" + bitext_unique_key).text)
		if "found" in response_bitext_unique_posneg and response_bitext_unique_posneg["found"]:
			previous_count = response_bitext_unique_posneg["_source"]["count"]
			previous_average_score = response_bitext_unique_posneg["_source"]["averageScore"]
			previous_categories = response_bitext_unique_posneg["_source"]["category"]
			separator = ", "
		update_query = {
			"doc":{
				"section": bitext_element["_source"]["section"],
				"averageScore": 1.0*(previous_average_score*previous_count + bitext_element["_source"]["score"])/(previous_count + 1),
				"count": previous_count + 1,
				"category": previous_categories + separator + bitext_element["_source"]["category"]
			},
			"doc_as_upsert": True
		}
		bitext_unique_response = requests.post("http://localhost:9200/bitext_unique_posneg/" + bitext_element["_source"]["hotelSequence"] + "/" + bitext_unique_key + "/_update", data = json.dumps(update_query))
		print(bitext_unique_response.text)
	#load the next batch of elements
	response = json.loads(requests.get("http://localhost:9200/_search/scroll?scroll=1m", data=scroll_response["_scroll_id"]).text)
	hits = response["hits"]["hits"]
	print("hits: " + str(len(hits)))
