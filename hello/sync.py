
import pymongo

from hello import database_host
from hello.data.context import data_context as context


class HelloPyMongo():

    def __init__(self):
        self.client = pymongo.MongoClient(database_host)
        """ The client connection
            :type: pymongo.MongoClient
        """
        self.db = self.client['motor_test']
        """ The client database
            :type: pymongo.Database
        """

    def insert_test(self):
        print('Starting')
        result = self.db.potato.insert(context.get_potato())
        print('Result: {}'.format(result))
        print('Stopping')


if __name__ == '__main__':
    HelloPyMongo().insert_test()
