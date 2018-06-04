#!/home/ubuntu/nifi_examples/nifi_ExecuteScript/venv/bin/python3

"""
FetchMongoId
ExecuteStreamCommand Processor Python script
"""

# Authors: Shu Yan <yanshu.usc@gmail.com>
# NiFi version: 1.6.0
# params: -u<uri>;-d<database>;-c<collection>[;-l<limit>]


import argparse
from mongo_utils import MongoUtils

parser = argparse.ArgumentParser(description='test')
parser.add_argument('-u', '--uri',
                    required=True, help='Mongo URI')
parser.add_argument('-d', '--database',
                    required=True, help='Mongo Database')
parser.add_argument('-c', '--collection',
                    required=True, help='Mongo Collection')
parser.add_argument('-l', '--limit',
                    type=int, default=0,
                    help='Fetch Limit')
# parse arguments
args = parser.parse_args()
# connect to mongo
mongo = MongoUtils(uri=args.uri.strip(),
                   database=args.database.strip(),
                   collection=args.collection.strip())
# return batches
for batch in mongo.fetch_id(limit=args.limit):
    print(batch)
