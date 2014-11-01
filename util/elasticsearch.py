import requests
import json
import time
import unittest

class Elasticsearch(object):
	"A class to handle a connection to Elasticsearch"

	url = None

	def __init__(self, host="localhost", port=9200):
		"Inits the url"
		self.url = "http://" + host + ":" + str(port)

	def is_up(self):
		"Checks if there is an elasticsearch instance up at the defined host/port."
		"Returns the info of the instance if up, False otherwise"
		try:
			result = json.loads(requests.get(self.url).text)
			if "status" in result and result["status"] == 200:
				return result
			return False
		except:
			return False

	def create_index(self, _index):
		"Creates a new index. Returns elasticsearch response"
		return json.loads(requests.put(self.url + "/" + _index).text)

	def remove_index(self, _index):
		"Removes an index. Returns elasticsearch response"
		return json.loads(requests.delete(self.url + "/" + _index).text)

	def list_indexes(self):
		"Lists the available indexes in elasticsearch. Returns elasticsearch response"
		return requests.get(self.url + "/_cat/indices?v").text

	def upsert(self, _index, _type, _id, document):
		"Updates a document in a certain index/type/id and returns elasticsearch response"
		query = {
            "doc":document,
            "doc_as_upsert":True
        }
		return json.loads(requests.post(self.url + "/" + _index + "/" + _type + "/" + _id + "/_update", data=json.dumps(query)).text)

	def iterate(self, index):
		"Returns an iterator for the specified index"
		return Iterator(index)


class Iterator(object):
	"An iterator for an Elasticsearch index"

	scroll_id = None
	elasticsearch = None

	def __init__(self, elasticsearch, index, pagesize=100):
		"Initializes the iterator"
		query = {
			"query":{
				"match_all":{}
			}
		}
		self.elasticsearch = elasticsearch
		scroll_id = json.loads(requests.get(self.elasticsearch.url + "/" + index + "/_search?search_type=scan&scroll=1m&size=" + str(pagesize), data=json.dumps(query)).text)["_scroll_id"]

	def next(self):
		"Returns the next batch of hits"
		return json.loads(requests.get(elasticsearch.url + "/_search/scroll?scroll=1m", data=self.scroll_id).text)

###############################################
################ UNIT TESTS ###################
###############################################
class ElasticsearchTests(unittest.TestCase):
	"Tests for elasticsearch. Only triggered when an elasticsearch server is up"

	host = "localhost"
	port = 9200
	index = "test_index_" + str(time.time())
	elasticsearch = Elasticsearch(host=host, port=port)

	@unittest.skipIf(not(elasticsearch.is_up()), "irrelevant test if there is no elasticsearch instance")
	def test_create_index(self):
		create_index = self.elasticsearch.create_index(self.index)
		self.assertTrue("acknowledged" in create_index)
		self.assertEquals(create_index["acknowledged"], True)
		index_list = self.elasticsearch.list_indexes()
		self.assertTrue(self.index in index_list)


if __name__ == '__main__':
    unittest.main()

