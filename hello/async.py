
from tornado.ioloop import IOLoop
from tornado import gen
from tornado.concurrent import Future
import motor

from hello import database_host
from hello.data.context import data_context as context


class HelloMotor():

    def __init__(self):
        """ No IO is made in the constructor.
        """

        self.client = motor.MotorClient(database_host)
        """ The client connection
            :type: motor.MotorClient
        """
        self.db = self.client['motor_test']
        """ The client database
            :type: motor.Database
        """
        self.ioloop = IOLoop.current()
        """ :type: IOLoop
        """

    def insert_with_callback_test(self):
        """ The most basic example I could figure out.
            Note that it is important to start an IOLoop, otherwise the callback will never be called. Motor is
            certainly depending on Tornardo's loop to run.
        """
        def on_insert(result, error):
            """ The callback called when the insert is complete.
            """
            if error is not None:
                print('Error: {}'.format(error))
            else:
                print('Result: {}'.format(result))
            self.ioloop.stop()

        self.db.potato.insert(context.get_potato(), callback=on_insert)
        print('Starting sync insert')
        self.ioloop.start()
        print('Stopped')

    def insert_with_future_test(self):
        """ Simple example using Future instead of passing a callback.
            Again, it is important to start an IOLoop.
        """
        @gen.coroutine
        def insert():
            print('Starting async insert')
            future = self.db.potato.insert(context.get_potato())
            """ :type: Future
            """
            yield future
            print('Result: {}'.format(future.result()))

        self.ioloop.run_sync(insert)
        print('Stopped')

    def bulk_insert_with_future_test(self):
        @gen.coroutine
        def bulk_insert():
            print('Starting bulk async insert')
            future = self.db.potato.insert(context.get_potato_bag())
            """ :type: Future
            """
            yield future

            print('Result: {}'.format(future.result()))
            future = self.db.potato.count()
            yield future

            print('Potato count: {}'.format(future.result()))

        self.ioloop.run_sync(bulk_insert)
        print('Stopped')


if __name__ == '__main__':
    # HelloMotor().insert_with_callback_test()
    # HelloMotor().insert_with_future_test()
    HelloMotor().bulk_insert_with_future_test()
