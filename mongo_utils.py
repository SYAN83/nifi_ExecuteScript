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

    def fetch_id(self, limit=0):
        batches = self._client.find_raw_batches(
            projection={"_id":1},
            sort=[('$natural',-1)],
            limit=limit)
        for batch in batches:
            yield [record['_id'] for record in bson.decode_all(batch)]

    def close(self):
        self.client.close()


if __name__ == '__main__':
    mongo = MongoUtils(uri='mongodb://jobs_user_readonly:jobPortalRead@ec2-52-14-242-71.us-east-2.compute.amazonaws.com:27017/',
                       database='jobs',
                       collection='job_location')
    for batch in mongo.fetch_id(limit=20):
        print(batch)