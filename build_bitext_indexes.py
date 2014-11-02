from util.trace import Trace
from util.elasticsearch import Elasticsearch
from util.csv_reader import CsvReader
import os.path
import unittest

class _Main(object):
	"Builds the three elasticsearch indexes of the bitext prototype and the relations among them"

	# path of the files
	filedir = os.path.dirname(os.path.realpath(__file__))
	hotels_file = os.path.join(filedir,"./data/hotels.csv")
	comments_file = os.path.join(filedir,"./data/comments.csv")
	bitext_file = os.path.join(filedir,"./data/bitext_tuipilot.csv")
	# indexes
	hotels_index = "hotels"
	comments_index = "comments"
	bitext_index = "bitext"
	bitext_unique_index = "bitext_unique"
	bitext_unique_posneg_index = "bitext_unique_posneg"
	# elasticsearch instance
	elasticsearch = Elasticsearch("localhost", 9200)

	def __init__(self, test=False):
		"Inits the script"

		Trace.info("Starting" + (" ", " test")[test] +" script...")
		# change paths and indexes in case of test
		if test:
			# path of the files
			self.hotels_file = os.path.join(filedir,"./data/hotels_test.csv")
			self.comments_file = os.path.join(filedir,"./data/comments_test.csv")
			self.bitext_file = os.path.join(filedir,"./data/bitext_tuipilot_test.csv")
			# indexes
			self.hotels_index = "test_hotels"
			self.comments_index = "test_comments"
			self.bitext_index = "test_bitext"
			self.bitext_unique_index = "test_bitext_unique"
			self.bitext_unique_posneg_index = "test_bitext_unique_posneg"
		
		# hotels first
		self.build_hotels_index()

	def build_hotels_index(self):
		Trace.info("Building hotels index...")
		# build the typemap
		hotels_keys = CsvReader.read_keys()
		hotels_typemap = dict(zip(hotels_keys[3:], [int]*len(hotels_keys[3:])))
		# get the bulk of documents
		hotels = CsvReader.read(self.hotels_file, typemap=hotels_typemap)
		Trace.info(str(len(hotels)) + " hotels read")
		# bulk_upsert
		hotels_upserted = elasticsearch.upsert_bulk(self.hotels_index, "destinationCode", "hotelSequence")
		Trace.info(str(hotels_upserted) + " hotels upserted")


###############################################
################ UNIT TESTS ###################
###############################################
class _MainTests(unittest.TestCase):
    "Main script unit tests"

    def test_script(self):
    	_Main(test = True)
    	elasticsearch = Elasticsearch("localhost", 9200)
    	# test hotels index
    	hotel148611 = elasticsearch.read_document("test_hotels", "BAI", "148611")
    	self.assertTrue(hotel148611["found"])
    	self.assertEquals(hotel148611["_source"]["mailsEnviados"], 11)
    	# test comments index
    	comment330952 = elasticsearch.read_document("test_comments", "148611", "330952")
    	self.assertTrue(comment330952["found"])
    	self.assertEquals(comment330952["_source"]["averageWebScore"], 4)
    	# test bitext index
    	count_bitext = elasticsearch.count("test_bitext")
    	self.assertEquals(count_bitext, 9)
    	last_bitext = elasticsearch.read_document("test_bitext", "POS", "9")
    	self.assertEquals(last_bitext["_source"]["score"], 2.0)
    	self.assertEquals(last_bitext["_source"]["mailsEnviados"], 37)
    	# test bitext_unique_posneg index
    	bitext330956POS = elasticsearch.read_document("test_bitext_unique_posneg", "69559", "330956POS")
    	self.assertTrue(bitext330956POS["found"])
    	self.assertEquals(bitext330956POS["_source"]["averageScore"], 2.0)
    	# test bitext_unique index
    	bitext330956 = elasticsearch.read_document("test_bitext_unique_posneg", "69559", "330956")
    	self.assertTrue(bitext330956["found"])
    	self.assertEquals(bitext330956["_source"]["averageScore"], 2.0)

    def tearDown(self):
    	# delete indexes
    	elasticsearch = Elasticsearch("localhost", 9200)
    	elasticsearch.remove_index("test_hotels")
    	elasticsearch.remove_index("test_comments")
    	elasticsearch.remove_index("test_bitext")
    	elasticsearch.remove_index("test_bitext_unique_posneg")
    	elasticsearch.remove_index("test_bitext_unique")

if __name__ == '__main__':
	if (sys.argv[1] == "test"):
		unittest.main()
	else:
    	_Main()