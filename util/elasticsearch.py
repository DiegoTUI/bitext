import requests
import json
import time
import os.path
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

	def upsert_document(self, _index, _type, _id, document):
		"Updates a document in a certain index/type/id and returns elasticsearch response"
		query = {
            "doc":document,
            "doc_as_upsert":True
        }
		return json.loads(requests.post(self.url + "/" + _index + "/" + _type + "/" + _id + "/_update", data=json.dumps(query)).text)

	def upsert_bulk(self, _index, type_key, id_key, bulk):
		"Updates a bulk of documents in the same index."
		"The type and id of each document are encoded in the document. Keys are provided to retrieve them"
		"The type and id fields are removed in the document after inserted as _type and _id"
		"Returns the number of documents upserted."
		docs_upserted = 0
		for document in bulk:
			_type = document[type_key]
			_id = document[id_key]
			del document[type_key]
			del document[id_key]
			upserted = self.upsert_document(_index, _type, _id, document)
			if (upserted["_id"] == _id):
				docs_upserted += 1
		return docs_upserted

	def read_document(self, _index, _type, _id):
		"Returns the document specified for the idex/type/id provided"
		return json.loads(requests.get(self.url + "/" + _index + "/" + _type + "/" + _id).text)

	def iterate(self, _index, pagesize=100):
		"Returns an iterator for the specified index and page size"
		return Iterator(self, _index, pagesize)


class Iterator(object):
	"An iterator for an Elasticsearch index"

	scroll_id = None
	elasticsearch = None

	def __init__(self, elasticsearch, index, pagesize):
		"Initializes the iterator"
		query = {
			"query":{
				"match_all":{}
			},
			"size":pagesize
		}
		self.elasticsearch = elasticsearch
		self.scroll_id = json.loads(requests.get(self.elasticsearch.url + "/" + index + "/_search?search_type=scan&scroll=1m", data=json.dumps(query)).text)["_scroll_id"]

	def next(self):
		"Returns the next batch of hits"
		return json.loads(requests.get(self.elasticsearch.url + "/_search/scroll?scroll=1m", data=self.scroll_id).text)

###############################################
################ UNIT TESTS ###################
###############################################
class ElasticsearchTests(unittest.TestCase):
	"Tests for elasticsearch. Only triggered when an elasticsearch server is up"

	host = "localhost"
	port = 9200
	_index = "test_index_" + str(time.time())
	_type = "test_type"
	doc1 = {"hotelId":"id1", "score": 2.5}
	doc2 = {"hotelId":"id2", "score": 3.7}
	filedir = os.path.dirname(os.path.realpath(__file__))

	elasticsearch = Elasticsearch(host=host, port=port)

	@unittest.skipIf(not(elasticsearch.is_up()), "irrelevant test if there is no elasticsearch instance")
	def test01_create_index(self):
		create_index = self.elasticsearch.create_index(self._index)
		self.assertTrue("acknowledged" in create_index)
		self.assertEquals(create_index["acknowledged"], True)
		index_list = self.elasticsearch.list_indexes()
		self.assertTrue(self._index in index_list)

	@unittest.skipIf(not(elasticsearch.is_up()), "irrelevant test if there is no elasticsearch instance")
	def test02_upsert_documents(self):
		upsert = self.elasticsearch.upsert_document(self._index, self._type, "1", self.doc1)
		self.assertEquals(upsert["_index"], self._index)
		self.assertEquals(upsert["_type"], self._type)
		self.assertEquals(upsert["_id"], "1")
		self.assertEquals(upsert["_version"], 1)
		upsert = self.elasticsearch.upsert_document(self._index, self._type, "2", self.doc2)
		self.assertEquals(upsert["_index"], self._index)
		self.assertEquals(upsert["_type"], self._type)
		self.assertEquals(upsert["_id"], "2")
		self.assertEquals(upsert["_version"], 1)
		self.doc1["score"] = 7.54
		upsert = self.elasticsearch.upsert_document(self._index, self._type, "1", self.doc1)
		self.assertEquals(upsert["_index"], self._index)
		self.assertEquals(upsert["_type"], self._type)
		self.assertEquals(upsert["_id"], "1")
		self.assertEquals(upsert["_version"], 2)

	@unittest.skipIf(not(elasticsearch.is_up()), "irrelevant test if there is no elasticsearch instance")
	def test03_upsert_bulk(self):
		bulk = json.loads(open(os.path.join(self.filedir, "../test/bulk.json")).read())
		add_type_array = [{"_type":self._type}] * len(bulk)
		bulk = [dict(x.items() + y.items()) for x,y in zip(bulk, add_type_array)]
		upserted = self.elasticsearch.upsert_bulk(self._index, "_type", "hotelId", bulk)
		self.assertEquals(upserted, len(bulk))
		doc = self.elasticsearch.read_document(self._index, self._type, "id10")
		self.assertEquals(doc["_index"], self._index)
		self.assertEquals(doc["_type"], self._type)
		self.assertEquals(doc["_id"], "1")
		self.assertTrue(doc["found"])
		self.assertFalse("_type" in doc["_source"])
		self.assertFalse("hotelId" in doc["_source"])

	@unittest.skipIf(not(elasticsearch.is_up()), "irrelevant test if there is no elasticsearch instance")
	def test04_read_document(self):
		doc = self.elasticsearch.read_document(self._index, self._type, "1")
		self.assertEquals(doc["_index"], self._index)
		self.assertEquals(doc["_type"], self._type)
		self.assertEquals(doc["_id"], "1")
		self.assertTrue(doc["found"])
		self.assertEquals(doc["_source"], self.doc1)

	@unittest.skipIf(not(elasticsearch.is_up()), "irrelevant test if there is no elasticsearch instance")
	def test05_remove_index(self):
		remove_index = self.elasticsearch.remove_index(self._index)
		self.assertTrue("acknowledged" in remove_index)
		self.assertEquals(remove_index["acknowledged"], True)
		index_list = self.elasticsearch.list_indexes()
		self.assertTrue(not (self._index in index_list))

if __name__ == '__main__':
    unittest.main()

