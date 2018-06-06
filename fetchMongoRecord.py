#!./venv/bin/python3

"""
FetchMongoId
ExecuteStreamCommand Processor Python script
"""

# Authors: Shu Yan <yanshu.usc@gmail.com>
# NiFi version: 1.6.0
# params: -u<uri>;-d<database>;-c<collection>[;-l<limit>]

import sys
import json
import argparse
from mongo_utils import MongoUtils

parser = argparse.ArgumentParser(description='Return the Mongo document IDs as a list.')
parser.add_argument('-u', '--uri',
                    required=True, help='Mongo URI')
parser.add_argument('-d', '--database',
                    required=True, help='the database to get a collection from')
parser.add_argument('-c', '--collection',
                    required=True, help='the name of the collection to get')
parser.add_argument('-s', '--skip',
                    type=int, default=0,
                    help='the number of documents to omit (from the start of the result set) when returning the results')
parser.add_argument('-l', '--limit',
                    type=int, default=0,
                    help='the maximum number of results to return')

# parse arguments
args = parser.parse_args()

# connect to mongo
mongo = MongoUtils(uri=args.uri.strip(),
                   database=args.database.strip(),
                   collection=args.collection.strip())
# return batches
try:
    # get number of record as skip
    if args.skip > 0:
        skip = args.skip
    else:
        for line in sys.stdin.readlines():
            skip = int(line.strip())
            break
    # skip = int(args.skip)
    # print Records to flowFile
    data = list()
    for obj in mongo.fetch(skip=skip, limit=args.limit):
        data.append(obj)
    # print records as JSON    
    print(json.dumps(data, indent=2))
    sys.exit(0)
except Exception as e:
    print(e)
    sys.exit(1)
