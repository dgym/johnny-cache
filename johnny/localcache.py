# -*- coding: utf-8 -*-

class LocalCache(object):
    '''
    Keeps a persistent local copy of cached data.

    Only selected keys (added by calling watch()) are cached.

    Knows enough about the generation scheme to remove all stale
    items when a generation changes.
    '''
    def __init__(self, cache_backend):
        self.cache_backend = cache_backend
        self.generations = {}
        self.stored = {}
        self.watched = set()

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

    def watch(self, key):
        self.watched.add(key)

    def __getattr__(self, key, *args):
        return getattr(self.cache_backend, key, *args)

    def get(self, key, default=None):
        if key in self.stored:
            return self.stored[key]

        value = self.cache_backend.get(key, default)
        if key in self.watched:
            self.stored[key] = value

        return value
