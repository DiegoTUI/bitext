from util.trace import Trace
from util.elasticsearch import Elasticsearch
from util.csv_manager import CsvManager
from datetime import date
from datetime import timedelta
import requests
import json
import sys
import unittest
import os

test_packages = None

class _Main(object):
    "Builds the npm_packages index"

    _index = "npm_packages"
    _offset = 0
    filedir = os.path.dirname(os.path.realpath(__file__))
    elasticsearch = Elasticsearch("localhost", 9200)

    def __init__(self, test=False, offset = 0):
        "Inits the script"

        global test_packages

        Trace.info("Starting" + (" ", " test")[test] +" script...")
        # change paths and indexes in case of test
        if test:
            test_packages_file = os.path.join(self.filedir,"./data/test_npm_package_names")
            test_packages = [item["test_package_name"] for item in CsvManager.read(test_packages_file)]
            self._index = "test_npm_packages"
            Trace.info("test_packages: " + json.dumps(test_packages))

        # set offset
        self._offset = offset
        
        # build npm_packages_index
        self.build_npm_packages_index()

        Trace.info(("S", "Test s")[test] + "cript finished.")

    def build_npm_packages_index(self):

        global test_packages
        package_names = []
        # check if testing
        if test_packages != None and len(test_packages) > 0:
            package_names = test_packages
            Trace.info("Testing. Packages reduced to: " + str(len(package_names)))
        else: #not testing
            # get all the docs
            Trace.info("grabbing all packages from npm registry...")
            packages = json.loads(requests.get("https://skimdb.npmjs.com/registry/_all_docs").text)["rows"]
            package_names = [item["id"] for item in packages]
            Trace.info(str(len(package_names)) + " total packages grabbed")
        # apply offset
        package_names = package_names[self._offset:]
        Trace.info("Offset. Packages reduced to: " + str(len(package_names)))
        # go through them and feed elasticsearch
        for package_name in package_names:
            Trace.info("processing package: " + package_name)
            try:
                self.process_package(package_name)
            except:
                print ("Error processing package: " + package_name + ": " + str(sys.exc_info()[0]))
                continue
            

    def process_package(self, package_name):
        _id = package_name.replace("/","_")
        # grab npmjs registry information
        npm_registry_info = json.loads(requests.get("http://registry.npmjs.org/" + package_name).text) 
        Trace.info("npm_registry_info processed ok")
        # grab npm-stat_info
        today = date.today()
        month_ago = today - timedelta(30)
        npm_stat_info = json.loads(requests.get("http://npm-stat.com/downloads/range/" + date.strftime(month_ago, "%Y-%m-%d") + ":" + date.strftime(today, "%Y-%m-%d") + "/" + package_name).text)
        Trace.info("npm_stat_info processed ok")
        # build the doc and feed elasticsearch
        # _type first. _type will be the repo of the package. "no_repo" in case there is no repo.
        _type = "no_repo"
        if ("repository" in npm_registry_info and "type" in npm_registry_info["repository"]):
            _type = npm_registry_info["repository"]["type"].replace("/","_")
        # init document with versions
        document = {
            "versions": 0
        }
        if "versions" in npm_registry_info:
            document["versions"] = len(npm_registry_info["versions"].keys())
        # calculate downloads
        downloads = [0]
        if "downloads" in npm_stat_info and len(npm_stat_info["downloads"]) > 0:
            downloads = [item["downloads"] for item in npm_stat_info["downloads"]]
        document["average_downloads"] = reduce(lambda x, y: x + y, downloads) / len(downloads)
        # insert document
        Trace.info("about to upsert")
        Trace.info(json.dumps(self.elasticsearch.upsert_document(self._index, _type, _id, document)))
        Trace.info("upserted")


###############################################
################ UNIT TESTS ###################
###############################################
class _MainTests(unittest.TestCase):
    "Main script unit tests"

    elasticsearch = Elasticsearch("localhost", 9200)

    @unittest.skipIf(not(elasticsearch.is_up()), "irrelevant test if there is no elasticsearch instance")
    def test_script(self):
        global test_packages
        _Main(test = True)
        # count documents
        self.assertTrue(self.elasticsearch.count_documents("test_npm_packages") > 0)
        # assert express
        express_package = self.elasticsearch.read_document("test_npm_packages", "_all", "express")
        self.assertTrue(express_package["found"])

    def tearDown(self):
        # delete indexes
        self.elasticsearch.remove_index("test_npm_packages")

if __name__ == '__main__':
    #unittest.main()
    if len(sys.argv) > 1 and sys.argv[1] == "test":
        Trace.info("test")
        unittest.main(argv=sys.argv[:1], exit=True)
    else:
        if len(sys.argv) > 1:    
            try:
                offset = int(sys.argv[1])
                Trace.info("main with offset: " + str(offset))
                _Main(offset = offset)
            except:
                pass
        else:
            Trace.info("main")
            _Main()