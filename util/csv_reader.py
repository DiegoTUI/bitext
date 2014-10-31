import csv
import os.path
from trace import Trace
import unittest

class CsvManager(object):
    "A class for managing csv files where the first line is the list of keys."
    "It receives a type casting dictionary to convert keys to types"
    "It produces an array of documents with typed values"

    @classmethod
    def read(cls, filename, delimiter=";", typemap={}):
        "Reads a CVS file."
        "The first line of the file is always the list of keys."
        "It receives a delimiter and a type casting dictionary to convert values to types."
        "If no type casting dictionary is passed, all the values are assumed to be strings."
        "Returns an array of documents (key, value) with typed values."
        
        if not os.path.isfile(filename): return None

        with open(filename) as csv_file:
            reader = csv.reader(csv_file, delimiter=delimiter)
            #read keys
            keys = reader.next()
            #prepare typemap
            typearray = [str] * len(keys)
            for key in typemap:
                try:
                    typearray[keys.index(key)] = typemap[key]
                except:
                    pass
            #init result
            result = []
            for line in reader:
                try:
                    typed_line = [typecast(line[i]) for i,typecast in enumerate(typearray)]
                except:
                    typed_line = line
                result.append(dict(zip(keys,typed_line)))
            return result

###############################################
################ UNIT TESTS ###################
###############################################
class CsvManagerTests(unittest.TestCase):
    "CsvManager unit tests"

    filedir = os.path.dirname(os.path.realpath(__file__))

    def test_unexisting_filename(self):
        result = CsvManager.read("unexisting/file/name");
        self.assertTrue(result is None)

    def test_no_typemap(self):
        result = CsvManager.read(os.path.join(self.filedir, "../test/test.csv"))
        self.assertEqual(len(result),2)
        self.assertEqual(result[0]["hotelId"], "11234")
        self.assertEqual(result[0]["hotelName"], "name1")
        self.assertEqual(result[0]["date"], "12/12/56")
        self.assertEqual(result[0]["score"], "4,00")
        self.assertEqual(result[1]["hotelId"], "1163")
        self.assertEqual(result[1]["hotelName"], "name2")
        self.assertEqual(result[1]["date"], "03/03/75")
        self.assertEqual(result[1]["score"], "5.56")

    def test_typemap(self):
        result = CsvManager.read(os.path.join(self.filedir, "../test/test.csv"), typemap={"score":float})
        self.assertEqual(len(result),2)
        self.assertEqual(result[0]["hotelId"], "11234")
        self.assertEqual(result[0]["hotelName"], "name1")
        self.assertEqual(result[0]["date"], "12/12/56")
        self.assertEqual(result[0]["score"], "4,00")
        self.assertEqual(result[1]["hotelId"], "1163")
        self.assertEqual(result[1]["hotelName"], "name2")
        self.assertEqual(result[1]["date"], "03/03/75")
        self.assertEqual(result[1]["score"], 5.56)

if __name__ == '__main__':
    unittest.main()



