import django
from django.db.models import Manager
from django.db.models.query import QuerySet


class QuerySetMixin(object):
    def __init__(self, *args, **kwargs):
        self._no_monkey.__init__(self, *args, **kwargs)
        self.query._cache_locally = False

    def cache_locally(self):
        clone = self._clone()
        clone.query._cache_locally = True
        return clone

    def _clone(self, *args, **kwargs):
        clone = self._no_monkey._clone(self, *args, **kwargs)
        setattr(
            clone.query,
            '_cache_locally',
            getattr(self.query, '_cache_locally', False),
        )
        return clone


class ManagerMixin(object):
    # Django 1.5- compatability
    if django.VERSION < (1, 6):
        get_queryset = Manager.get_query_set

    def cache_locally(self):
        return self.get_queryset().cache_locally()


def patch():
    '''Adds a cache_locally() method to QuerySet and Manager.

    This is picked up by the QueryCacheBackend and causes
    the results to be stored persistently in local memory.
    '''
    from utils import monkey_mix
    monkey_mix(QuerySet, QuerySetMixin)
    monkey_mix(Manager, ManagerMixin)
