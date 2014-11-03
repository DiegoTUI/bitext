#!/bin/bash
python ./util/csv_manager.py
python ./util/elasticsearch.py
python ./build_bitext_indexes.py test