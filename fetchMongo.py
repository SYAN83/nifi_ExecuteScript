"""
FetchMongo ExecuteScript
"""

# Authors: Shu Yan <yanshu.usc@gmail.com>
# NiFi Ver: 1.6.0

# Required Property:
#     MongoURI
#     MongoDatabaseName
#     MongoCollectionName
#     Sort: None [["$natural",-1]],
#     Limit: 0
#     BatchSize 0

# import nifi libraries
from org.apache.commons.io import IOUtils
from java.nio.charset import StandardCharsets
from org.apache.nifi.processor.io import OutputStreamCallback

# import python libraries
import bson, json
import pymongo

import os
os.getpid()

# MongoDB utility
class MongoUtils(object):

    def __init__(self, uri, database, collection, limit=0):
        self._client = MongoUtils.db_connect(uri, database, collection)
        self.limit = limit

    @staticmethod
    def db_connect(uri, database, collection):
        return pymongo.MongoClient(uri)[database][collection]

    def fetch_id(self, limit=0):
        batches = self._client.find_raw_batches(
            projection={"_id":1},
            sort=[('$natural',-1)],
            limit=limit)
        for batch in batches:
            yield [record['_id'] for record in bson.decode_all(batch)]

    def close(self):
        self.client.close()


# Define a subclass of OutputStreamCallback for use in session.write()
class PyOutputStreamCallback(OutputStreamCallback):

    def __init__(self, obj):
        self.obj = obj

    def process(self, outputStream):
        outputStream.write(bytearray(json.dumps(self.obj).encode('utf-8')))

# Get flowFile (trigger)
flowFile = session.get()

if flowFile:
    # Get required property
    mongo_property = {
        'uri': MongoURI.evaluateAttributeExpressions(flowFile).getValue(),
        'database': MongoDatabaseName.evaluateAttributeExpressions(flowFile).getValue(),
        'collection': MongoCollectionName.evaluateAttributeExpressions(flowFile).getValue(),
    }
    mongo = MongoUtils(**mongo_property)
    limit =  Limit.evaluateAttributeExpressions(flowFile).getValue()
    # Connect to MongoDB server
    try:
        flowFile = session.write(flowFile, PyOutputStreamCallback(mongo_property))
        session.transfer(flowFile, REL_SUCCESS)
        # Generate flowFile batches
        # for batch in mongo.fetch_id(limit=limit):
        #     newFlowFile = session.create(flowFile)
        #     newFlowFile = session.write(newFlowFile, PyOutputStreamCallback(batch))
        #     session.transfer(newFlowFile, REL_SUCCESS)
    except Exception as e:
        log.error(e)
        session.transfer(flowFile, REL_FAILURE)