#!/usr/bin/env python
# -*- coding: utf-8 -*-

from django.core.cache.backends.locmem import LocMemCache

import base
from johnny.localcache import LocalCache


class LocalCacheTestCase(base.JohnnyTestCase):
    def setUp(self):
        self.backend = LocMemCache('', {})
        self.cache = LocalCache(self.backend)

    def prime(self, key, value, limit=None):
        self.backend.set(key, value)
        self.cache.watch(key, limit)
        self.cache.get(key)
        self.backend.delete(key)

    def test_fall_through(self):
        self.cache.set('a', '1')
        self.assertEqual(self.cache.get('a'), '1')
        self.assertIsNone(self.cache.get('b'))
        self.assertFalse(self.cache.stored)

    def test_watch(self):
        self.backend.set('a', '1')
        self.cache.watch('a')
        self.assertEqual(self.cache.get('a'), '1')
        self.backend.clear()
        self.assertEqual(self.cache.get('a'), '1')

    def test_watch_not_there(self):
        NOT_THERE = object()
        self.cache.watch('a')
        self.assertIs(self.cache.get('a', NOT_THERE), NOT_THERE)
        self.backend.set('a', '1')
        self.assertIs(self.cache.get('a', NOT_THERE), NOT_THERE)

    def test_watch_limit(self):
        NOT_THERE = object()
        self.prime('a', [range(5)], 5)
        self.assertEqual(self.cache.get('a'), [range(5)])
        self.cache.clear()
        self.prime('a', [range(5)], 4)
        self.assertIs(self.cache.get('a', NOT_THERE), NOT_THERE)

    def test_watch_limit_strange_values(self):
        self.prime('a', '1', 5)
        self.assertEqual(self.cache.get('a'), '1')

        self.cache.clear()
        self.prime('a', [], 5)
        self.assertEqual(self.cache.get('a'), [])

        self.cache.clear()
        self.prime('a', ['1'], 5)
        self.assertEqual(self.cache.get('a'), ['1'])

    def test_purge(self):
        self.cache.check_generation(['t1'], 't1a')
        self.cache.watch('prefix_t1a.a')
        self.cache.watch('prefix_t1a.b')
        self.cache.check_generation(['t2'], 't2a')
        self.cache.watch('prefix_t2a.a')
        self.cache.watch('prefix_t2a.b')

        self.cache.get('prefix_t1a.a')
        self.cache.get('prefix_t1a.b')
        self.cache.get('prefix_t2a.a')

        self.assertEqual(len(self.cache.stored), 4)
        self.cache.check_generation(['t1'], 't1b')
        self.assertEqual(len(self.cache.stored), 2)
        self.cache.check_generation(['t2'], 't2b')
        self.assertEqual(len(self.cache.stored), 0)
