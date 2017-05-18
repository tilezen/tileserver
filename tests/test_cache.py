import unittest


class CacheHelperTests(unittest.TestCase):
    def test_clean_empty_parent_dirs(self):
        import os
        from tileserver.cache import clean_empty_parent_dirs, mkdir_p

        self.assertFalse(
            os.path.exists('foo'),
            'The test expects foo/ to not exist when it starts'
        )

        mkdir_p('foo/bar/baz')
        with open('foo/bar/baz/hello.txt', 'w') as f:
            f.write('hello world')
        clean_empty_parent_dirs('foo/bar/baz/hello.txt')
        self.assertTrue(
            os.path.exists('foo/bar/baz/hello.txt'),
            "The directory is not empty, so it should not have been deleted")
        os.remove('foo/bar/baz/hello.txt')

        clean_empty_parent_dirs('foo/bar/baz', 'foo/bar')
        self.assertTrue(
            os.path.exists('foo/bar'),
            "Shouldn't have deleted the parent_dir")

        clean_empty_parent_dirs('foo/bar')
        self.assertFalse(
            os.path.exists('foo'),
            "Should have deleted everything")


class FileCacheTests(unittest.TestCase):
    def test_obtain_lock(self):
        from ModestMaps.Core import Coordinate
        from tileserver.cache import CacheKey, FileCache
        from tilequeue.format import lookup_format_by_extension

        coord = Coordinate(0, 0, 0)
        tile_size = 1
        layers = 'all'
        fmt = lookup_format_by_extension('mvt')
        cache_key = CacheKey(coord, tile_size, layers, fmt)

        c = FileCache('foo')
        try:
            c.obtain_lock(cache_key)
        finally:
            c.release_lock(cache_key)

    def test_obtain_lock_already_locked(self):
        from ModestMaps.Core import Coordinate
        from tileserver.cache import CacheKey, FileCache, LockTimeout
        from tilequeue.format import lookup_format_by_extension

        coord = Coordinate(0, 0, 0)
        tile_size = 1
        layers = 'all'
        fmt = lookup_format_by_extension('mvt')
        cache_key = CacheKey(coord, tile_size, layers, fmt)

        c = FileCache('foo')
        try:
            # Obtain a lock from "client A"
            c.obtain_lock(cache_key)
            with self.assertRaises(LockTimeout):
                # Locking from "client B" should time out
                c.obtain_lock(cache_key, timeout=1)
        finally:
            c.release_lock(cache_key)

        # After releasing, obtaining the lock from "client A" should work
        c.obtain_lock(cache_key)
        c.release_lock(cache_key)

    def test_contextmanager_lock(self):
        from ModestMaps.Core import Coordinate
        from tileserver.cache import CacheKey, FileCache, LockTimeout
        from tilequeue.format import lookup_format_by_extension

        coord = Coordinate(0, 0, 0)
        tile_size = 1
        layers = 'all'
        fmt = lookup_format_by_extension('mvt')
        cache_key = CacheKey(coord, tile_size, layers, fmt)

        c = FileCache('foo')

        # A plain 'ol lock should work without exception
        with c.lock(cache_key):
            pass

        with c.lock(cache_key):
            with self.assertRaises(LockTimeout):
                # A second lock on the same coord should time out
                with c.lock(cache_key, timeout=1):
                    pass

    def test_set_get(self):
        import os
        from ModestMaps.Core import Coordinate
        from tileserver.cache import CacheKey, FileCache, \
            clean_empty_parent_dirs
        from tilequeue.format import lookup_format_by_extension

        coord = Coordinate(0, 0, 0)
        tile_size = 1
        layers = 'all'
        fmt = lookup_format_by_extension('mvt')
        cache_key = CacheKey(coord, tile_size, layers, fmt)

        tile_data = 'hello world'

        c = FileCache('foo')
        c.set(cache_key, tile_data)
        actual_data = c.get(cache_key)

        self.assertEquals(tile_data, actual_data)

        key = c._generate_key('data', cache_key)
        os.remove(key)
        clean_empty_parent_dirs(os.path.dirname(key))


class MockRedis(object):
    def __init__(self):
        self._data = {}

    def set(self, key, data):
        self._data[key] = data

    def get(self, key):
        return self._data.get(key)

    def delete(self, key):
        del self._data[key]

    def setnx(self, key, data):
        if key in self._data:
            return False
        else:
            self.set(key, data)

    def getset(self, key, data):
        val = self._data.get(key)
        self._data[key] = data
        return val


class RedisCacheTests(unittest.TestCase):
    def setUp(self):
        self.redis = MockRedis()

    def test_obtain_lock(self):
        from ModestMaps.Core import Coordinate
        from tileserver.cache import CacheKey, RedisCache
        from tilequeue.format import lookup_format_by_extension

        coord = Coordinate(0, 0, 0)
        tile_size = 1
        layers = 'all'
        fmt = lookup_format_by_extension('mvt')
        cache_key = CacheKey(coord, tile_size, layers, fmt)

        c = RedisCache(self.redis)
        try:
            c.obtain_lock(cache_key)
        finally:
            c.release_lock(cache_key)

    def test_obtain_lock_already_locked(self):
        from ModestMaps.Core import Coordinate
        from tileserver.cache import CacheKey, RedisCache, LockTimeout
        from tilequeue.format import lookup_format_by_extension

        coord = Coordinate(0, 0, 0)
        fmt = lookup_format_by_extension('mvt')
        tile_size = 1
        layers = 'all'
        cache_key = CacheKey(coord, tile_size, layers, fmt)

        c = RedisCache(self.redis)
        try:
            c.obtain_lock(cache_key)
            with self.assertRaises(LockTimeout):
                c.obtain_lock(cache_key, timeout=1)
        finally:
            c.release_lock(cache_key)

        c.obtain_lock(cache_key)

    def test_contextmanager_lock(self):
        from ModestMaps.Core import Coordinate
        from tileserver.cache import CacheKey, RedisCache, LockTimeout
        from tilequeue.format import lookup_format_by_extension

        coord = Coordinate(0, 0, 0)
        fmt = lookup_format_by_extension('mvt')
        tile_size = 1
        layers = 'all'
        cache_key = CacheKey(coord, tile_size, layers, fmt)

        c = RedisCache(self.redis)

        # A plain 'ol lock should work without exception
        with c.lock(cache_key):
            pass

        with c.lock(cache_key):
            with self.assertRaises(LockTimeout):
                # A second lock on the same coord should time out
                with c.lock(cache_key, timeout=1):
                    pass

    def test_set_get(self):
        from ModestMaps.Core import Coordinate
        from tileserver.cache import CacheKey, RedisCache
        from tilequeue.format import lookup_format_by_extension

        coord = Coordinate(0, 0, 0)
        fmt = lookup_format_by_extension('mvt')
        tile_size = 1
        layers = 'all'
        cache_key = CacheKey(coord, tile_size, layers, fmt)
        tile_data = 'hello world'

        c = RedisCache(self.redis)
        c.set(cache_key, tile_data)
        actual_data = c.get(cache_key)

        self.assertEquals(tile_data, actual_data)
        self.redis.delete(
            c._generate_key('data', cache_key))
