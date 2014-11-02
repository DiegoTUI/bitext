from util.trace import Trace
from util.elasticsearch import Elasticsearch
from util.csv_reader import CsvReader
import os.path

class _Main(object):
	"Builds the three elasticsearch indexes of the bitext prototype and the relations among them"

	def __init__(self):
		Trace.info("Starting script...")
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
		# elasticsearch instance
		elasticsearch = Elasticsearch("localhost", 9200)
		# hotels first
		Trace.info("Building hotels index...")
		# build the typemap
		hotels_keys = CsvReader.read_keys()
		hotels_typemap = dict(zip(hotels_keys[3:], [int]*len(hotels_keys[3:])))
		# get the bulk of documents
		hotels = CsvReader.read(os.path.join(filedir,"./data/hotels.csv"), typemap=hotels_typemap)
		Trace.info(str(len(hotels)) + " hotels read")
		# bulk_upsert
		hotels_upserted = elasticsearch.upsert_bulk(hotels_index, "destinationCode", "hotelSequence")
		Trace.info(str(hotels_upserted) + " hotels upserted")




if __name__ == '__main__':
    _Main()