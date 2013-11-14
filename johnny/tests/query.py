#!/usr/bin/env python
# -*- coding: utf-8 -*-

import base


class CacheLocallyTestCase(base.JohnnyTestCase):
    def test_manager(self):
        from testapp.models import Genre
        qs = Genre.objects.cache_locally()
        self.assertTrue(qs.query._cache_locally)

    def test_queryset(self):
        from testapp.models import Genre
        qs = Genre.objects.all()
        self.assertFalse(qs.query._cache_locally)

        qs = qs.cache_locally()
        self.assertTrue(qs.query._cache_locally)

    def test_clone(self):
        from testapp.models import Genre
        qs = Genre.objects.all()
        qs = qs.cache_locally()
        self.assertTrue(qs.query._cache_locally)

        qs = qs.order_by('id')
        self.assertTrue(qs.query._cache_locally)
