# -*- coding: utf-8 -*-

class LocalCache(object):
    '''
    Keeps a persistent local copy of cached data.

    Only selected keys (added by calling watch()) are cached.

    Knows enough about the generation scheme to remove all stale
    items when a generation changes.
    '''

    NOT_THERE = object()

    def __init__(self, cache_backend):
        self.cache_backend = cache_backend
        self.generations = {}
        self.stored = {}

    def check_generation(self, tables, generation):
        tables_key = ','.join(sorted(tables))
        old_gen = self.generations.setdefault(tables_key, generation)
        if old_gen == generation:
            return

        expired = []
        for key in self.stored:
            gen = key.rsplit('_', 1)[1].split('.')[0]
            if gen == old_gen:
                expired.append(key)

        for key in expired:
            del self.stored[key]

        self.generations[tables_key] = generation

    def watch(self, key, limit=None):
        value = self.stored.get(key)

        if value:
            value['limit'] = limit
        else:
            self.stored[key] = {'limit': limit}

    def __getattr__(self, key, *args):
        return getattr(self.cache_backend, key, *args)

    def clear(self):
        for item in self.stored.values():
            item.pop('value', None)
        return self.cache_backend.clear()

    def get(self, key, default=None):
        item = self.stored.get(key)

        if not item:
            return self.cache_backend.get(key, default)

        def real_value(value):
            if value is self.NOT_THERE:
                return default
            return value

        if 'value' in item:
            return real_value(item['value'])

        value = self.cache_backend.get(key, self.NOT_THERE)

        if item:
            if item['limit'] is not None:
                if value is self.NOT_THERE:
                    count = 0
                elif isinstance(value, (tuple, list)):
                    if len(value) > 0 and isinstance(value[0], (tuple, list)):
                        count = len(value[0])
                    else:
                        count = len(value)
                else:
                    count = 1
                if count <= item['limit']:
                    item['value'] = value
            else:
                item['value'] = value

        return real_value(value)
