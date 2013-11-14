#!/usr/bin/env python
# -*- coding: utf-8 -*-

from django.core.cache.backends.locmem import LocMemCache

import base
from johnny.localcache import LocalCache


class LocalCacheTestCase(base.JohnnyTestCase):
    def setUp(self):
        self.backend = LocMemCache('', {})
        self.cache = LocalCache(self.backend)

    def test_fall_through(self):
        self.cache.set('a', '1')
        self.assertEqual(self.cache.get('a'), '1')
        self.assertIsNone(self.cache.get('b'))
        self.assertFalse(self.cache.stored)

    def test_watch(self):
        self.cache.watch('a')
        self.assertFalse(self.cache.stored)
        self.assertEqual(self.cache.get('a'), '1')
        self.assertEqual(self.cache.stored, {'a': '1'})

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
        self.cache.get('prefix_t2a.b')

        self.assertEqual(len(self.cache.stored), 4)
        self.cache.check_generation(['t1'], 't1b')
        self.assertEqual(len(self.cache.stored), 2)
        self.cache.check_generation(['t2'], 't2b')
        self.assertEqual(len(self.cache.stored), 0)
