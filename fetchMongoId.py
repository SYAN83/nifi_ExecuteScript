#!./venv/bin/python3

"""
FetchMongoId
ExecuteStreamCommand Processor Python script
"""

# Authors: Shu Yan <yanshu.usc@gmail.com>
# NiFi version: 1.6.0
# params: -u<uri>;-d<database>;-c<collection>[;-l<limit>]

import sys
import argparse
from mongo_utils import MongoUtils

parser = argparse.ArgumentParser(description='Return the Mongo document IDs as a list.')
parser.add_argument('-u', '--uri',
                    required=True, help='Mongo URI')
parser.add_argument('-d', '--database',
                    required=True, help='Name of the Mongo Database')
parser.add_argument('-c', '--collection',
                    required=True, help='Name of the Mongo Collection')
parser.add_argument('-l', '--limit',
                    type=int, default=0,
                    help='Limited by the number of the most recent inserted documents.')

# parse arguments
args = parser.parse_args()
# connect to mongo
mongo = MongoUtils(uri=args.uri.strip(),
                   database=args.database.strip(),
                   collection=args.collection.strip())
# return batches
try:
    for batch in mongo.fetch_id(limit=args.limit):
        print(batch)
    sys.exit(0)
except Exception as e:
    print(e)
    sys.exit(1)
