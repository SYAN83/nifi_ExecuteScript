import pymongo
import bson
import json


# MongoDB utility
class MongoUtils(object):

    def __init__(self, uri, database, collection):
        self._client = MongoUtils.db_connect(uri, database, collection)

    @staticmethod
    def db_connect(uri, database, collection):
        return pymongo.MongoClient(uri)[database][collection]

    def fetch(self, skip=0, limit=0):
        for record in self._client.find(skip=skip, limit=limit, show_record_id=False):
            yield record

    def fetch_id(self, limit=0):
        batches = self._client.find_raw_batches(
            projection={"_id":1},
            sort=[('$natural',-1)],
            limit=limit)
        for batch in batches:
            yield [record['_id'] for record in bson.decode_all(batch)]

    def insert(self, data):
        documents = json.loads(data)
        print(documents)
        resp = self._client.insert_many(documents=documents)
        print(resp)

    def close(self):
        self.client.close()


