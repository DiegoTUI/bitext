import csv
import unittest

class CsvManager(object):
    "A class for managing csv files where the first line is the list of keys."
    "It receives a type casting dictionary to convert keys to types"
    "It produces an array of documents with typed values"

    @classmethod
    def read(filename, delimiter=";", typemap=None):
        "Reads a CVS file."
        "The first line of the file is always the list of keys."
        "It receives a delimiter and a type casting dictionary to convert values to types."
        "If no type casting dictionary is passed, all the values are assumed to be strings."
        "Returns an array of documents (key, value) with typed values."
        with open(filename) as csv_file:
            reader = csv.reader(csv_file, delimiter=delimiter)
            keys = reader.next()
            result = []
            for line in reader:
                # TYPE CASTING MISSING!!!
                result.append(dict(zip(keys,line)))
            return result



