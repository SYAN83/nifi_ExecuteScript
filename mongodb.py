import pymongo
try:
    import urllib.parse
except ImportError:
    import urllib


class MongoUtils():
    """
    Write data to mongoDB, data type must be python dict
    """
    client = None

    def __init__(self, **kwargs):

        if self.client is None:
            self.db_connect(**mongo_config)
        self.collection = collection
        self.items = self.client[mongo_config['database']][collection]
        # find primary keys in mongoDB
        for item in self.items.find({},{self.key: 1}):
            self._id.add(item[self.key])

    def db_connect(self, username, password, host, port, database):
        username = urllib.parse.quote_plus(username)
        password = urllib.parse.quote_plus(password)
        self.client = pymongo.MongoClient('mongodb://{}:{}@{}:{}/{}'.format(username, password, host, port, database))

    def write(self, item):
        if not self.validate(item):
            return
        try:
            post_id = self.items.insert_one(item).inserted_id
            logging.info('collection: {}, _id: {}, time: {}'.format(self.collection, post_id, int(time.time())))
        except pymongo.errors.DuplicateKeyError:
            logging.warning('Duplicate Key found when inserting, skipped.')

    def close(self):
        # MongoDBWriter instances share the same connection, don't disconnect when job finishes.
        pass
        # self.client.close()


if __name__ == '__main__':
    pass