
import collections


class DataContext():

    @staticmethod
    def get_potato(number: int=0) -> dict:
        """ Returns a potato.
        """
        return {'number': number}

    @staticmethod
    def get_potato_bag(count: int=10) -> collections.Iterable:
        """ Returns a generator of a bag of potatoes.
            :rtype: collections.Iterable[dict]
        """
        return ({'number': i} for i in range(count))


data_context = DataContext
