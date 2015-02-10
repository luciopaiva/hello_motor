""" Simple Motor examples.

    Reference: http://motor.readthedocs.org/en/latest/tutorial.html
"""

import collections

from tornado.ioloop import IOLoop
from tornado import gen
from tornado.concurrent import Future
import motor

from hello import database_host
from hello.data.context import data_context as context


class SmartCursor(collections.Iterable):
    """ Encapsulate Motor's cursor to add functionality to it.
        This proxy can return a Future to a document, what Motor's original cursor isn't capable of doing.
    """

    def __init__(self, motor_cursor, callback=None):
        self.motor_cursor = motor_cursor
        self.map_chain = []
        self.callback = callback
        # Uncomment to test batch_size:
        # self.motor_cursor.batch_size(10)

    def __getattr__(self, name):
        """ Proxy everything else to Motor's cursor.
        """
        return getattr(self.motor_cursor, name)

    def _post_process(self, document):
        if document is not None:
            for method in self.map_chain:
                document = method(document)
        return document

    def fetch_object(self):
        """ Returns a Future to a document or raises StopInteraction if the cursor is exhausted.
        """
        future = Future()

        if not self.motor_cursor._buffer_size() and self.motor_cursor.alive:
            if self.motor_cursor._empty():
                # Special case, limit of 0
                # future.set_result(False)
                future.set_exception(StopIteration())
                return future

            def cb(batch_size, error):
                if batch_size == 0:
                    future.set_exception(StopIteration())
                elif error:
                    future.set_exception(error)
                else:
                    document = next(self.motor_cursor.delegate)
                    future.set_result(self._post_process(document))

            self.motor_cursor._get_more(cb)
            return future
        elif self.motor_cursor._buffer_size():
            document = next(self.motor_cursor.delegate)
            future.set_result(self._post_process(document))
            return future
        else:
            # Dead
            # future.set_result(False)
            future.set_exception(StopIteration())
        return future

    def __iter__(self):
        return self

    def __next__(self):
        return self.fetch_object()

    def send(self, value):
        if self.callback is not None:
            self.callback(value)
        return next(self)

    def throw(self, exc_type, value, traceback):
        raise exc_type(value)

    def to_list(self, length=None, callback=None):
        """ MotorCursor.to_list() requires you to pass length even if you want to pass None. This proxy method just
            sets length to None by default, so you may call cursor.to_list() instead of cursor.to_list(None).
        """
        return self.motor_cursor.to_list(length=length, callback=callback)

    def map(self, method):
        self.map_chain.append(method)
        return self

    def has_next(self):
        return self.motor_cursor.fetch_next

    def get_next(self):
        document = self.motor_cursor.next_object()
        return self._post_process(document)


class HelloMotor():

    def __init__(self):
        """ No IO is made in the constructor.
        """

        self.client = motor.MotorClient(database_host)
        """ The client connection.
            :type: motor.MotorClient
        """
        self.db = self.client['motor_test']
        """ The client database
            :type: motor.Database
        """
        self.ioloop = IOLoop.current()
        """ :type: IOLoop
        """

    def test_insert_with_callback_test(self):
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

    def test_insert_with_future_test(self):
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

    def test_bulk_insert_with_future_test(self):
        """ Bulk insert example.
        """
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

    def test_find_one_potato(self):
        """ Find One example.
        """
        @gen.coroutine
        def find_one():
            print('Starting search for a potato')
            document = yield self.db.potato.find_one({'number': 1})
            print('A potato was found (id: {})'.format(document['_id']))

        self.ioloop.run_sync(find_one)
        print('Stopped')

    def test_find_another_potato(self):
        """ Same test as find_one_potato(), but this test shows that it is necessary to annotate all methods down to
            the innermost method that yields a Future.
        """
        @gen.coroutine
        def find_another_inner():
            print('Starting search for another potato')
            document = yield self.db.potato.find_one({'number': 1})
            print('A potato was found (id: {})'.format(document['_id']))

        @gen.coroutine
        def find_another():
            """ Remove the coroutine annotation and the yield directive from this method to confirm that Tornado loses
                the task and is unable to resume after the yield in find_another_inner().
            """
            yield find_another_inner()

        self.ioloop.run_sync(find_another)
        print('Stopped')

    def test_find_some_potatoes(self):
        """ Find example. Shows how to use count() too.
        """
        @gen.coroutine
        def find_some():
            print('Starting search for some potatoes')
            cursor = self.db.potato.find({'number': {'$gt': 8}})
            """ :type: motor.MotorCursor
            """
            while (yield cursor.fetch_next):
                potato = cursor.next_object()
                print('A potato was found (id: {})'.format(potato['_id']))
            count = yield cursor.count()
            print('Potatoes found: {}'.format(count))

        self.ioloop.run_sync(find_some)
        print('Stopped')

    def test_find_some_potatoes_with_generator(self):
        """ I thought this was a good approach, but it throws when the last Future is yielded without a document to
            fetch.

            Compare it to the ``find_some_potatoes()`` test.
        """

        def get_potatoes():
            print('Starting search for some potatoes')
            return SmartCursor(self.db.potato.find({'number': {'$gt': 8}}))\
                .map(lambda potato: potato['number'])\
                .map(lambda number: number + 1)

        @gen.coroutine
        def find_with_gen():

            print('Should print this line before iterating')

            try:
                for future in get_potatoes():
                    potato = yield future
                    print('> {}'.format(potato))
            except StopIteration:
                print('StopIteration was raised')
            print('Should print this line after iterating')

        self.ioloop.run_sync(find_with_gen)
        print('Stopped')

    def test_find_some_potatoes_with_yield_from(self):
        @gen.coroutine
        def find_with_yield_from():
            def on_document(potato):
                print('> {}'.format(potato))

            print('Should print this line before yielding')
            yield from SmartCursor(self.db.potato.find({'number': {'$gt': 8}}), on_document)
            print('Should print this line after yielding')

        self.ioloop.run_sync(find_with_yield_from)
        print('Stopped')

    def test_find_some_potatoes_to_list(self):
        @gen.coroutine
        def find_to_list():

            cursor = SmartCursor(self.db.potato.find({'number': {'$gt': 8}}))\
                .map(lambda pot: pot['number'])\
                .map(lambda number: number + 1)

            print('Should print this line before yielding')
            potatoes = yield cursor.to_list()
            print('Should print this line after yielding')

            for potato in potatoes:
                print('> {}'.format(potato))

        self.ioloop.run_sync(find_to_list)
        print('Stopped')

    def test_find_some_potatoes_with_while(self):
        @gen.coroutine
        def find_with_while():
            cursor = SmartCursor(self.db.potato.find({'number': {'$gt': 8}}))
            print('Should print this line before while')
            while (yield cursor.has_next()):
                potato = cursor.get_next()
                print('> {}'.format(potato))
            print('Should print this line after while')

        self.ioloop.run_sync(find_with_while)
        print('Stopped')

    def test_update_potatoes(self):
        """ Update example.
        """
        @gen.coroutine
        def update():
            print('Starting update')
            result = yield self.db.potato.update(
                {'number': {'$gt': 8}},
                {'$set': {'updated': True}},  # use $set to really update and not replace
                multi=True)                   # do this for all potatoes that match
            print(result)

            cursor = self.db.potato.find({'updated': True})
            """ :type: motor.MotorCursor
            """
            while (yield cursor.fetch_next):
                potato = cursor.next_object()
                print('Potato updated ({})'.format(potato))

        self.ioloop.run_sync(update)
        print('Stopped')

    def test_remove_potatoes(self):
        """ Remove example.
        """
        @gen.coroutine
        def remove():
            print('Inserting test documents')
            potatoes_to_remove = yield self.db.potato.insert(context.get_potato_bag())
            print('Done inserting')

            print('Starting remove')
            result = yield self.db.potato.remove({'_id': {'$in': potatoes_to_remove}})
            print('Done removing')
            print(result)

        self.ioloop.run_sync(remove)
        print('Stopped')


def get_testable_methods(obj):
    for m in (m for m in dir(obj) if callable(getattr(obj, m)) and m.startswith('test')):
        yield getattr(obj, m)


def test_all(obj):
    for method in get_testable_methods(obj):
        print('-' * 80)
        print(method.__name__)
        print('-' * 80)
        method()

if __name__ == '__main__':
    # test_all(HelloMotor())
    HelloMotor().test_find_some_potatoes_with_while()
